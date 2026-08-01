//! Atomic publication of canonical M18 embedding Arrow results.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use graphforge_core::uuid::Uuid;
use graphforge_search::{
    EmbeddingRefreshLimits, EmbeddingRefreshRequest, EmbeddingSourceCaptureLimits,
    capture_embedding_source, refresh_embedding_generation,
};
use graphforge_storage::{
    EmbeddingBatchRow, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityId,
    EmbeddingCompatibilityInput, EmbeddingDisplayName, EmbeddingDistance, EmbeddingNormalization,
    EmbeddingProducerIdentity, EmbeddingSourceState, EmbeddingValueType, SearchArtifactError,
    SearchSourcePart, ValidatedEmbeddingBatch, VectorStoreLimits, validate_embedding_batch,
};

use super::{Algorithm, AnalyzeAlgorithm, EmbeddingSpaceInfo, GfError, GraphForge, NodeSelector};

const ALGORITHM_SCHEMA_VERSION: &str = "1";
const RNG_VERSION: &str = "splitmix64-v1";
const RNG_DERIVATION: &str = "graphforge-embedding-substream-v1";
const SOURCE_PART_NAME: &str = "m18_result_uuid_projection_v1";

/// Persisted normalization selected for a canonical M18 embedding result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum M18EmbeddingNormalization {
    /// Preserve validated M18 coordinates exactly.
    #[default]
    None,
    /// Normalize every row to unit L2 norm before persistence.
    L2,
}

/// Retrieval distance selected for a canonical M18 embedding result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum M18EmbeddingDistance {
    /// Exact cosine similarity.
    #[default]
    Cosine,
}

/// One complete canonical M18 embedding Arrow result to publish.
///
/// The type deliberately omits `Debug`: the Arrow batch contains primary
/// vector data and must not enter routine diagnostic output.
pub struct M18EmbeddingPublicationRequest {
    /// Durable caller-facing alias bound after complete publication.
    pub display_name: String,
    /// M18 embedding algorithm that produced `result`.
    pub algorithm: AnalyzeAlgorithm,
    /// Frozen M18 algorithm contract version.
    pub algorithm_version: String,
    /// Fixed Float32 vector width, retained for an empty result.
    pub dimensions: u32,
    /// Persisted normalization contract.
    pub normalization: M18EmbeddingNormalization,
    /// Persisted retrieval distance contract.
    pub distance: M18EmbeddingDistance,
    /// Normalized M18 algorithm hyperparameters participating in identity.
    pub hyperparameters: BTreeMap<String, serde_json::Value>,
    /// Non-empty versioned M18 input recipe participating in identity.
    pub input_recipe: BTreeMap<String, serde_json::Value>,
    /// Non-empty graph-projection recipe participating in identity.
    pub source_projection_recipe: BTreeMap<String, serde_json::Value>,
    /// Exact canonical `node_uuid`, `embedding` Arrow result.
    pub result: RecordBatch,
    /// Permit rebinding `display_name` from another compatibility lineage.
    pub replace_alias: bool,
}

struct PreparedM18Publication {
    display_name: String,
    replace_alias: bool,
    descriptor: EmbeddingCompatibilityDescriptor,
    rows: Vec<EmbeddingBatchRow>,
    dimensions: usize,
    normalization: EmbeddingNormalization,
    vector_limits: VectorStoreLimits,
    projection_bytes: Vec<u8>,
}

impl GraphForge {
    /// Validate and atomically publish one complete canonical M18 embedding result.
    ///
    /// The result schema and M18 metadata are exact, every UUID is revalidated
    /// against this graph on each bounded attempt, and the display alias is
    /// bound only after a complete generation is active. Exact replay reuses
    /// the immutable generation.
    ///
    /// # Errors
    /// Returns structured validation, execution, lifecycle, storage,
    /// cancellation, corruption, locking, and resource-limit errors. Failed
    /// validation or publication never mutates the alias catalog.
    pub fn publish_m18_embeddings(
        &self,
        request: M18EmbeddingPublicationRequest,
    ) -> Result<EmbeddingSpaceInfo, GfError> {
        let prepared = self.prepare_m18_publication(request)?;
        let eligible_count = u64::try_from(prepared.rows.len()).map_err(|_| {
            GfError::Execution("M18 embedding row count cannot be represented".to_owned())
        })?;
        let project_dir = self.dir.clone();
        let projection_bytes = prepared.projection_bytes.clone();
        self.publish_prepared_m18_embeddings(
            &prepared,
            move || {
                capture_embedding_source(
                    &project_dir,
                    &[SearchSourcePart {
                        name: SOURCE_PART_NAME,
                        bytes: &projection_bytes,
                    }],
                    &[],
                    eligible_count,
                    EmbeddingSourceCaptureLimits::default(),
                    || Ok(()),
                )
            },
            || Ok(()),
        )
    }

    fn prepare_m18_publication(
        &self,
        request: M18EmbeddingPublicationRequest,
    ) -> Result<PreparedM18Publication, GfError> {
        EmbeddingDisplayName::new(&request.display_name)?;
        require_m18_embedding_algorithm(request.algorithm)?;
        validate_canonical_m18_schema(
            &request.result,
            request.algorithm,
            &request.algorithm_version,
            request.dimensions,
            &request.hyperparameters,
        )?;

        let dimensions = usize::try_from(request.dimensions)
            .map_err(|_| validation("M18 embedding dimensions cannot be represented"))?;
        let vector_limits = VectorStoreLimits::default();
        graphforge_storage::vector_schema(dimensions, vector_limits)?;
        let normalization = storage_normalization(request.normalization);
        let distance = storage_distance(request.distance);
        let rows = decode_m18_rows(&request.result, dimensions, vector_limits)?;
        let validated =
            self.validate_owned_m18_batch(&rows, dimensions, normalization, vector_limits)?;
        let projection_bytes = validated
            .rows()
            .iter()
            .flat_map(|row| row.node_uuid)
            .collect();

        let descriptor = EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::M18 {
                algorithm: Algorithm::Analyze(request.algorithm).as_str().to_owned(),
                algorithm_version: request.algorithm_version,
            },
            dimensions: request.dimensions,
            value_type: EmbeddingValueType::Float32,
            normalization,
            distance,
            tokenizer: None,
            chunking: None,
            hyperparameters: request.hyperparameters,
            input_recipe: request.input_recipe,
            source_projection_recipe: request.source_projection_recipe,
        })?;
        self.preflight_m18_alias(
            &request.display_name,
            descriptor.compatibility_id()?,
            request.replace_alias,
        )?;
        Ok(PreparedM18Publication {
            display_name: request.display_name,
            replace_alias: request.replace_alias,
            descriptor,
            rows,
            dimensions,
            normalization,
            vector_limits,
            projection_bytes,
        })
    }

    fn validate_owned_m18_batch(
        &self,
        rows: &[EmbeddingBatchRow],
        dimensions: usize,
        normalization: EmbeddingNormalization,
        vector_limits: VectorStoreLimits,
    ) -> Result<ValidatedEmbeddingBatch, GfError> {
        let mut eligible = BTreeSet::new();
        for row in rows {
            self.resolve_node_selector(&NodeSelector::Uuid(Uuid::from_bytes(row.node_uuid)))?;
            eligible.insert(row.node_uuid);
        }
        validate_embedding_batch(
            rows.to_vec(),
            &eligible,
            dimensions,
            normalization,
            vector_limits,
            || Ok(()),
        )
        .map_err(Into::into)
    }

    fn preflight_m18_alias(
        &self,
        display_name: &str,
        compatibility_id: EmbeddingCompatibilityId,
        replace_alias: bool,
    ) -> Result<(), GfError> {
        if replace_alias {
            return Ok(());
        }
        if self.embedding_spaces()?.iter().any(|space| {
            space.aliases.iter().any(|alias| alias == display_name)
                && space.compatibility_id != compatibility_id.to_hex()
        }) {
            return Err(validation(
                "embedding alias already targets another compatibility identity; explicit replacement is required",
            ));
        }
        Ok(())
    }

    fn publish_prepared_m18_embeddings<S, C>(
        &self,
        prepared: &PreparedM18Publication,
        capture_source: S,
        checkpoint: C,
    ) -> Result<EmbeddingSpaceInfo, GfError>
    where
        S: FnMut() -> Result<EmbeddingSourceState, SearchArtifactError>,
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let now = transaction_time_micros();
        refresh_embedding_generation(
            &self.dir,
            EmbeddingRefreshRequest {
                descriptor: &prepared.descriptor,
                generated_at_micros: now,
                committed_at_micros: now,
            },
            EmbeddingRefreshLimits::default(),
            capture_source,
            |_| {
                self.validate_owned_m18_batch(
                    &prepared.rows,
                    prepared.dimensions,
                    prepared.normalization,
                    prepared.vector_limits,
                )
                .map_err(search_producer_error)
            },
            checkpoint,
        )?;
        self.bind_embedding_space_alias(
            &prepared.display_name,
            &prepared.descriptor.compatibility_id()?.to_hex(),
            prepared.replace_alias,
        )
    }
}

fn require_m18_embedding_algorithm(algorithm: AnalyzeAlgorithm) -> Result<(), GfError> {
    if matches!(
        algorithm,
        AnalyzeAlgorithm::Node2Vec
            | AnalyzeAlgorithm::GraphSage
            | AnalyzeAlgorithm::FastRandomProjection
            | AnalyzeAlgorithm::HashGnn
    ) {
        Ok(())
    } else {
        Err(validation(
            "M18 embedding publication requires an embedding analysis algorithm",
        ))
    }
}

fn validate_canonical_m18_schema(
    result: &RecordBatch,
    algorithm: AnalyzeAlgorithm,
    algorithm_version: &str,
    dimensions: u32,
    hyperparameters: &BTreeMap<String, serde_json::Value>,
) -> Result<(), GfError> {
    let dimensions_i32 = i32::try_from(dimensions)
        .map_err(|_| validation("M18 embedding dimensions exceed the Arrow i32 range"))?;
    let schema = result.schema();
    let expected = [
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
    if schema.fields().len() != expected.len()
        || schema
            .fields()
            .iter()
            .zip(expected.iter())
            .any(|(actual, expected)| actual.as_ref() != expected)
    {
        return Err(validation(
            "M18 embedding result must have exact node_uuid and embedding fields",
        ));
    }
    let metadata = schema.metadata();
    let expected_algorithm = Algorithm::Analyze(algorithm);
    let seed = normalized_seed(hyperparameters)?;
    let expected_metadata = BTreeMap::from([
        (
            "graphforge.algorithm",
            expected_algorithm.as_str().to_owned(),
        ),
        ("graphforge.verb", "analyze".to_owned()),
        ("graphforge.algorithm_version", algorithm_version.to_owned()),
        (
            "graphforge.algorithm_schema_version",
            ALGORITHM_SCHEMA_VERSION.to_owned(),
        ),
        ("graphforge.dimensions", dimensions.to_string()),
        ("graphforge.seed", seed.to_string()),
        ("graphforge.rng_version", RNG_VERSION.to_owned()),
        ("graphforge.rng_derivation", RNG_DERIVATION.to_owned()),
    ]);
    if metadata.len() != expected_metadata.len()
        || expected_metadata
            .iter()
            .any(|(key, value)| metadata.get(*key).map(String::as_str) != Some(value.as_str()))
    {
        return Err(validation(
            "M18 embedding result has non-canonical algorithm metadata",
        ));
    }
    Ok(())
}

fn decode_m18_rows(
    result: &RecordBatch,
    dimensions: usize,
    limits: VectorStoreLimits,
) -> Result<Vec<EmbeddingBatchRow>, GfError> {
    if result.num_rows() > limits.stored_vectors {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "embedding_rows",
            limit: usize_limit(limits.stored_vectors),
        }
        .into());
    }
    let cells = result.num_rows().checked_mul(dimensions).ok_or(
        SearchArtifactError::ResourceExhausted {
            resource: "embedding_vector_cells",
            limit: usize_limit(limits.vector_cells),
        },
    )?;
    if cells > limits.vector_cells {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "embedding_vector_cells",
            limit: usize_limit(limits.vector_cells),
        }
        .into());
    }
    let uuids = result
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| validation("M18 node_uuid column is malformed"))?;
    let embeddings = result
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| validation("M18 embedding column is malformed"))?;
    if uuids.null_count() != 0 || embeddings.null_count() != 0 {
        return Err(validation("M18 embedding result contains null rows"));
    }

    let mut rows = Vec::with_capacity(result.num_rows());
    for row in 0..result.num_rows() {
        let node_uuid = uuids
            .value(row)
            .try_into()
            .map_err(|_| validation("M18 result contains malformed node_uuid bytes"))?;
        let values = embeddings.value(row);
        let values = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| validation("M18 embedding row is not Float32"))?;
        if values.null_count() != 0 {
            return Err(validation("M18 embedding row contains null coordinates"));
        }
        if values.len() != dimensions {
            return Err(validation(format!(
                "M18 embedding row has dimension {}, expected {dimensions}",
                values.len()
            )));
        }
        rows.push(EmbeddingBatchRow {
            node_uuid,
            vector: values.values().to_vec(),
        });
    }
    Ok(rows)
}

fn normalized_seed(hyperparameters: &BTreeMap<String, serde_json::Value>) -> Result<u64, GfError> {
    hyperparameters.get("seed").map_or(Ok(0), |seed| {
        seed.as_u64()
            .ok_or_else(|| validation("M18 embedding seed must be an unsigned 64-bit integer"))
    })
}

fn search_producer_error(error: GfError) -> SearchArtifactError {
    match error {
        GfError::Validation(reason) => SearchArtifactError::InvalidSelector {
            field: "M18 embedding result",
            reason,
        },
        GfError::Storage(reason) => SearchArtifactError::SourceSnapshot { reason },
        GfError::Execution(reason) => SearchArtifactError::Build(reason),
        other => SearchArtifactError::Build(other.to_string()),
    }
}

const fn storage_normalization(value: M18EmbeddingNormalization) -> EmbeddingNormalization {
    match value {
        M18EmbeddingNormalization::None => EmbeddingNormalization::None,
        M18EmbeddingNormalization::L2 => EmbeddingNormalization::L2,
    }
}

const fn storage_distance(value: M18EmbeddingDistance) -> EmbeddingDistance {
    match value {
        M18EmbeddingDistance::Cosine => EmbeddingDistance::Cosine,
    }
}

fn transaction_time_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
        })
}

fn usize_limit(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::{HashMap, HashSet};

    use arrow::array::{FixedSizeListBuilder, Float32Builder, ListBuilder};
    use arrow::datatypes::Schema;
    use graphforge_storage::{
        EmbeddingProducerIdentity, EmbeddingSpaceDiscoveryLimits, VectorStoreLimits,
        discover_embedding_spaces, read_vector_snapshot,
    };

    use super::*;
    use crate::{EmbeddingSpaceProducer, PropValue};

    fn node(graph: &GraphForge, name: &str) -> crate::NodeHandle {
        graph
            .add_node(
                "Document",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap()
    }

    fn algorithm_version(algorithm: AnalyzeAlgorithm) -> &'static str {
        match algorithm {
            AnalyzeAlgorithm::Node2Vec => "node2vec-v1",
            AnalyzeAlgorithm::GraphSage => "graphsage-unsupervised-v1",
            AnalyzeAlgorithm::FastRandomProjection => "fastrp-v1",
            AnalyzeAlgorithm::HashGnn => "hashgnn-v1",
            _ => "not-an-embedding-v1",
        }
    }

    fn canonical_batch(
        algorithm: AnalyzeAlgorithm,
        dimensions: u32,
        rows: &[([u8; 16], Vec<f32>)],
    ) -> RecordBatch {
        let uuids = if rows.is_empty() {
            FixedSizeBinaryArray::new_null(16, 0)
        } else {
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|(uuid, _)| uuid.as_slice()))
                .unwrap()
        };
        let width = i32::try_from(dimensions).unwrap();
        let mut embeddings = FixedSizeListBuilder::new(Float32Builder::new(), width)
            .with_field(Arc::new(Field::new("item", DataType::Float32, false)));
        for (_, vector) in rows {
            for value in vector {
                embeddings.values().append_value(*value);
            }
            embeddings.append(true);
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        width,
                    ),
                    false,
                ),
            ],
            HashMap::from([
                (
                    "graphforge.algorithm".to_owned(),
                    Algorithm::Analyze(algorithm).as_str().to_owned(),
                ),
                ("graphforge.verb".to_owned(), "analyze".to_owned()),
                (
                    "graphforge.algorithm_version".to_owned(),
                    algorithm_version(algorithm).to_owned(),
                ),
                (
                    "graphforge.algorithm_schema_version".to_owned(),
                    ALGORITHM_SCHEMA_VERSION.to_owned(),
                ),
                ("graphforge.dimensions".to_owned(), dimensions.to_string()),
                ("graphforge.seed".to_owned(), "7".to_owned()),
                ("graphforge.rng_version".to_owned(), RNG_VERSION.to_owned()),
                (
                    "graphforge.rng_derivation".to_owned(),
                    RNG_DERIVATION.to_owned(),
                ),
            ]),
        ));
        RecordBatch::try_new(schema, vec![Arc::new(uuids), Arc::new(embeddings.finish())]).unwrap()
    }

    fn request(
        display_name: &str,
        algorithm: AnalyzeAlgorithm,
        dimensions: u32,
        result: RecordBatch,
    ) -> M18EmbeddingPublicationRequest {
        M18EmbeddingPublicationRequest {
            display_name: display_name.to_owned(),
            algorithm,
            algorithm_version: algorithm_version(algorithm).to_owned(),
            dimensions,
            normalization: M18EmbeddingNormalization::None,
            distance: M18EmbeddingDistance::Cosine,
            hyperparameters: BTreeMap::from([("seed".to_owned(), serde_json::json!(7))]),
            input_recipe: BTreeMap::from([("kind".to_owned(), serde_json::json!("structural"))]),
            source_projection_recipe: BTreeMap::from([(
                "label".to_owned(),
                serde_json::json!("Document"),
            )]),
            result,
            replace_alias: false,
        }
    }

    fn source(generation: u64, count: u64) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [generation as u8; 32], [8; 32], count)
    }

    fn assert_missing_alias(graph: &GraphForge, alias: &str) {
        assert!(matches!(
            graph.embedding_space(Some(alias)),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn canonical_result_is_idempotent_uuid_keyed_and_reopenable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let alice = node(&graph, "alice");
        let bob = node(&graph, "bob");
        let rows = vec![
            (*bob.uuid.as_bytes(), vec![3.0, 4.0]),
            (*alice.uuid.as_bytes(), vec![1.0, 2.0]),
        ];
        let first = graph
            .publish_m18_embeddings(request(
                "structural",
                AnalyzeAlgorithm::Node2Vec,
                2,
                canonical_batch(AnalyzeAlgorithm::Node2Vec, 2, &rows),
            ))
            .unwrap();
        let replay = graph
            .publish_m18_embeddings(request(
                "structural",
                AnalyzeAlgorithm::Node2Vec,
                2,
                canonical_batch(AnalyzeAlgorithm::Node2Vec, 2, &rows),
            ))
            .unwrap();
        assert_eq!(first, replay);
        assert!(matches!(
            first.producer,
            EmbeddingSpaceProducer::M18 {
                ref algorithm,
                ref algorithm_version,
            } if algorithm == "node2vec" && algorithm_version == "node2vec-v1"
        ));

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(reopened.embedding_space(Some("structural")).unwrap(), first);
        let discovered = discover_embedding_spaces(
            &reopened.dir,
            EmbeddingSpaceDiscoveryLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            discovered[0].descriptor().producer(),
            EmbeddingProducerIdentity::M18 { .. }
        ));
        let active = discovered[0].active().unwrap();
        let stored =
            read_vector_snapshot(&active.path, 2, VectorStoreLimits::default(), || Ok(())).unwrap();
        assert_eq!(stored.len(), 2);
        assert_eq!(
            stored
                .iter()
                .map(|row| row.node_uuid)
                .collect::<HashSet<_>>(),
            HashSet::from([*alice.uuid.as_bytes(), *bob.uuid.as_bytes()])
        );
        let metadata = format!("{first:?}");
        assert!(!metadata.contains("[1.0, 2.0]"));
        assert!(!metadata.contains("knowledge"));
    }

    #[test]
    fn empty_result_preserves_declared_dimension() {
        let graph = GraphForge::new(None).unwrap();
        let info = graph
            .publish_m18_embeddings(request(
                "empty",
                AnalyzeAlgorithm::HashGnn,
                7,
                canonical_batch(AnalyzeAlgorithm::HashGnn, 7, &[]),
            ))
            .unwrap();
        assert_eq!(info.dimensions, 7);
        assert_eq!(info.active.unwrap().vector_count, 0);
    }

    #[test]
    fn malformed_or_foreign_results_fail_before_alias_mutation() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alice");
        let other = GraphForge::new(None).unwrap();
        let foreign = node(&other, "foreign");

        let wrong_algorithm = request(
            "wrong-algorithm",
            AnalyzeAlgorithm::IsDag,
            2,
            canonical_batch(
                AnalyzeAlgorithm::IsDag,
                2,
                &[(*alice.uuid.as_bytes(), vec![1.0, 2.0])],
            ),
        );
        let wrong_dimension = request(
            "wrong-dimension",
            AnalyzeAlgorithm::Node2Vec,
            3,
            canonical_batch(
                AnalyzeAlgorithm::Node2Vec,
                2,
                &[(*alice.uuid.as_bytes(), vec![1.0, 2.0])],
            ),
        );
        let duplicate = request(
            "duplicate",
            AnalyzeAlgorithm::Node2Vec,
            2,
            canonical_batch(
                AnalyzeAlgorithm::Node2Vec,
                2,
                &[
                    (*alice.uuid.as_bytes(), vec![1.0, 2.0]),
                    (*alice.uuid.as_bytes(), vec![3.0, 4.0]),
                ],
            ),
        );
        let foreign = request(
            "foreign",
            AnalyzeAlgorithm::Node2Vec,
            2,
            canonical_batch(
                AnalyzeAlgorithm::Node2Vec,
                2,
                &[(*foreign.uuid.as_bytes(), vec![1.0, 2.0])],
            ),
        );
        let non_finite = request(
            "non-finite",
            AnalyzeAlgorithm::Node2Vec,
            2,
            canonical_batch(
                AnalyzeAlgorithm::Node2Vec,
                2,
                &[(*alice.uuid.as_bytes(), vec![f32::NAN, 2.0])],
            ),
        );
        let wrong_metadata_batch = canonical_batch(
            AnalyzeAlgorithm::Node2Vec,
            2,
            &[(*alice.uuid.as_bytes(), vec![1.0, 2.0])],
        );
        let wrong_metadata_schema = Arc::new(Schema::new_with_metadata(
            wrong_metadata_batch.schema().fields().to_vec(),
            HashMap::from([
                ("graphforge.algorithm".to_owned(), "graphsage".to_owned()),
                ("graphforge.verb".to_owned(), "analyze".to_owned()),
                (
                    "graphforge.algorithm_schema_version".to_owned(),
                    ALGORITHM_SCHEMA_VERSION.to_owned(),
                ),
            ]),
        ));
        let wrong_metadata_batch = RecordBatch::try_new(
            wrong_metadata_schema,
            wrong_metadata_batch.columns().to_vec(),
        )
        .unwrap();
        let wrong_metadata = request(
            "wrong-metadata",
            AnalyzeAlgorithm::Node2Vec,
            2,
            wrong_metadata_batch,
        );
        let canonical = canonical_batch(
            AnalyzeAlgorithm::Node2Vec,
            2,
            &[(*alice.uuid.as_bytes(), vec![1.0, 2.0])],
        );
        let mut extra_metadata = canonical.schema().metadata().clone();
        extra_metadata.insert("graphforge.extra".to_owned(), "forbidden".to_owned());
        let extra_metadata = request(
            "extra-metadata",
            AnalyzeAlgorithm::Node2Vec,
            2,
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    canonical.schema().fields().to_vec(),
                    extra_metadata,
                )),
                canonical.columns().to_vec(),
            )
            .unwrap(),
        );
        let mut missing_metadata = canonical.schema().metadata().clone();
        missing_metadata.remove("graphforge.rng_derivation");
        let missing_metadata = request(
            "missing-metadata",
            AnalyzeAlgorithm::Node2Vec,
            2,
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    canonical.schema().fields().to_vec(),
                    missing_metadata,
                )),
                canonical.columns().to_vec(),
            )
            .unwrap(),
        );
        let mut variable_embeddings = ListBuilder::new(Float32Builder::new())
            .with_field(Arc::new(Field::new("item", DataType::Float32, false)));
        variable_embeddings.values().append_value(1.0);
        variable_embeddings.values().append_value(2.0);
        variable_embeddings.append(true);
        let variable_list = request(
            "variable-list",
            AnalyzeAlgorithm::Node2Vec,
            2,
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    vec![
                        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                        Field::new(
                            "embedding",
                            DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
                            false,
                        ),
                    ],
                    canonical.schema().metadata().clone(),
                )),
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(
                            [alice.uuid.as_bytes().as_slice()].into_iter(),
                        )
                        .unwrap(),
                    ),
                    Arc::new(variable_embeddings.finish()),
                ],
            )
            .unwrap(),
        );
        let mut null_embeddings = ListBuilder::new(Float32Builder::new())
            .with_field(Arc::new(Field::new("item", DataType::Float32, false)));
        null_embeddings.append(false);
        let null_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(
                    "embedding",
                    DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
                    true,
                ),
            ],
            canonical.schema().metadata().clone(),
        ));
        let null_batch = RecordBatch::try_new(
            null_schema,
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        [alice.uuid.as_bytes().as_slice()].into_iter(),
                    )
                    .unwrap(),
                ),
                Arc::new(null_embeddings.finish()),
            ],
        )
        .unwrap();
        let null_row = request("null-row", AnalyzeAlgorithm::Node2Vec, 2, null_batch);
        for request in [
            wrong_algorithm,
            wrong_dimension,
            duplicate,
            foreign,
            non_finite,
            wrong_metadata,
            extra_metadata,
            missing_metadata,
            variable_list,
            null_row,
        ] {
            let alias = request.display_name.clone();
            assert!(graph.publish_m18_embeddings(request).is_err());
            assert_missing_alias(&graph, &alias);
        }
        assert!(graph.embedding_spaces().unwrap().is_empty());
    }

    #[test]
    fn retry_and_cancellation_preserve_atomic_alias_binding() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alice");
        let prepared = graph
            .prepare_m18_publication(request(
                "retry",
                AnalyzeAlgorithm::FastRandomProjection,
                2,
                canonical_batch(
                    AnalyzeAlgorithm::FastRandomProjection,
                    2,
                    &[(*alice.uuid.as_bytes(), vec![1.0, 2.0])],
                ),
            ))
            .unwrap();
        let states = [source(1, 1), source(2, 1), source(2, 1), source(2, 1)];
        let index = Cell::new(0);
        let published = graph
            .publish_prepared_m18_embeddings(
                &prepared,
                || {
                    let current = index.get();
                    index.set(current + 1);
                    Ok(states[current])
                },
                || Ok(()),
            )
            .unwrap();
        assert_eq!(index.get(), 4);
        assert_eq!(published.active.unwrap().source_graph_generation, 2);

        let unstable = graph
            .prepare_m18_publication(request(
                "unstable",
                AnalyzeAlgorithm::FastRandomProjection,
                2,
                canonical_batch(
                    AnalyzeAlgorithm::FastRandomProjection,
                    2,
                    &[(*alice.uuid.as_bytes(), vec![1.0, 2.0])],
                ),
            ))
            .unwrap();
        let states = [source(3, 1), source(4, 1), source(4, 1), source(5, 1)];
        let index = Cell::new(0);
        let error = graph
            .publish_prepared_m18_embeddings(
                &unstable,
                || {
                    let current = index.get();
                    index.set(current + 1);
                    Ok(states[current])
                },
                || Ok(()),
            )
            .unwrap_err();
        assert!(matches!(error, GfError::Lifecycle(_)));
        assert_missing_alias(&graph, "unstable");

        let cancelled = graph
            .prepare_m18_publication(request(
                "cancelled",
                AnalyzeAlgorithm::GraphSage,
                2,
                canonical_batch(
                    AnalyzeAlgorithm::GraphSage,
                    2,
                    &[(*alice.uuid.as_bytes(), vec![3.0, 4.0])],
                ),
            ))
            .unwrap();
        let error = graph
            .publish_prepared_m18_embeddings(
                &cancelled,
                || Ok(source(3, 1)),
                || Err(SearchArtifactError::Cancelled),
            )
            .unwrap_err();
        assert!(matches!(error, GfError::Execution(_)));
        assert_missing_alias(&graph, "cancelled");
    }
}
