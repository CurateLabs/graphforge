//! Graph-owned publication of complete caller-supplied embedding batches.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use gf_search::{
    EmbeddingRefreshLimits, EmbeddingRefreshRequest, EmbeddingSourceCaptureLimits,
    capture_embedding_source, refresh_embedding_generation,
};
use gf_storage::{
    EmbeddingBatchRow, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityId,
    EmbeddingCompatibilityInput, EmbeddingDisplayName, EmbeddingDistance, EmbeddingNormalization,
    EmbeddingProducerIdentity, EmbeddingSourceState, EmbeddingValueType, SearchArtifactError,
    SearchSourcePart, ValidatedEmbeddingBatch, VectorStoreLimits, validate_embedding_batch,
};

use super::{EmbeddingSpaceInfo, GfError, GraphForge, NodeSelector};

const CALLER_INPUT_KIND: &str = "complete_uuid_vector_batch";
const SOURCE_PART_NAME: &str = "resolved_uuid_projection_v1";

/// One graph-owned node selector paired with caller-supplied Float32 coordinates.
///
/// The type deliberately omits `Debug`: selectors may contain property values
/// and vectors are primary data rather than diagnostic metadata.
#[derive(Clone, PartialEq)]
pub struct CallerEmbeddingBatchRow {
    /// Selector resolved against this exact [`GraphForge`] instance.
    pub node: NodeSelector,
    /// Finite, non-zero fixed-width coordinates.
    pub vector: Vec<f32>,
}

/// Persisted normalization selected for a caller-supplied embedding space.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CallerEmbeddingNormalization {
    /// Preserve validated coordinates exactly.
    #[default]
    None,
    /// Normalize every row to unit L2 norm before persistence.
    L2,
}

/// Retrieval distance selected for a caller-supplied embedding space.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CallerEmbeddingDistance {
    /// Exact cosine similarity.
    #[default]
    Cosine,
}

/// Complete caller-supplied embedding generation to validate and publish.
///
/// The type deliberately omits `Debug` so vectors and property-match selector
/// values cannot leak through routine request logging.
#[derive(Clone, PartialEq)]
pub struct CallerEmbeddingBatchRequest {
    /// Durable caller-facing alias to bind after successful publication.
    pub display_name: String,
    /// Stable caller batch contract version participating in compatibility.
    pub contract_version: String,
    /// Fixed Float32 vector width, retained for an empty projection.
    pub dimensions: u32,
    /// Persisted normalization contract.
    pub normalization: CallerEmbeddingNormalization,
    /// Persisted retrieval distance contract.
    pub distance: CallerEmbeddingDistance,
    /// Canonical graph-projection recipe participating in compatibility.
    pub source_projection_recipe: BTreeMap<String, String>,
    /// Complete selected UUID/vector projection.
    pub rows: Vec<CallerEmbeddingBatchRow>,
    /// Permit rebinding `display_name` from another compatibility lineage.
    pub replace_alias: bool,
}

struct PreparedCallerPublication {
    display_name: String,
    replace_alias: bool,
    descriptor: EmbeddingCompatibilityDescriptor,
    rows: Vec<CallerEmbeddingBatchRow>,
    dimensions: usize,
    normalization: EmbeddingNormalization,
    vector_limits: VectorStoreLimits,
    projection_bytes: Vec<u8>,
}

impl GraphForge {
    /// Publish one complete caller-supplied embedding generation atomically.
    ///
    /// Selectors are resolved to graph-owned UUIDs before storage. The complete
    /// batch and compatibility descriptor are validated before publication,
    /// source capture retries once on concurrent committed mutation, and the
    /// requested alias is bound only after a complete generation is active.
    /// Exact replay reuses its immutable generation.
    ///
    /// # Errors
    /// Returns structured validation, execution, lifecycle, storage,
    /// cancellation, corruption, locking, and resource-limit errors. No failed
    /// validation or publication mutates the alias catalog.
    pub fn publish_caller_embeddings(
        &self,
        request: CallerEmbeddingBatchRequest,
    ) -> Result<EmbeddingSpaceInfo, GfError> {
        let prepared = self.prepare_caller_publication(request)?;
        let eligible_count = u64::try_from(prepared.rows.len()).map_err(|_| {
            GfError::Execution("caller embedding row count cannot be represented".to_owned())
        })?;
        let project_dir = self.dir.clone();
        self.publish_prepared_caller_embeddings(
            &prepared,
            move |projection_bytes| {
                capture_embedding_source(
                    &project_dir,
                    &[SearchSourcePart {
                        name: SOURCE_PART_NAME,
                        bytes: projection_bytes,
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

    fn prepare_caller_publication(
        &self,
        request: CallerEmbeddingBatchRequest,
    ) -> Result<PreparedCallerPublication, GfError> {
        EmbeddingDisplayName::new(&request.display_name)?;
        let dimensions = usize::try_from(request.dimensions)
            .map_err(|_| validation("caller embedding dimensions cannot be represented"))?;
        let vector_limits = VectorStoreLimits::default();
        gf_storage::vector_schema(dimensions, vector_limits)?;
        if request.rows.len() > vector_limits.stored_vectors {
            return Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_rows",
                limit: usize_limit(vector_limits.stored_vectors),
            }
            .into());
        }
        let vector_cells = request.rows.len().checked_mul(dimensions).ok_or(
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_vector_cells",
                limit: usize_limit(vector_limits.vector_cells),
            },
        )?;
        if vector_cells > vector_limits.vector_cells {
            return Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_vector_cells",
                limit: usize_limit(vector_limits.vector_cells),
            }
            .into());
        }
        let normalization = storage_normalization(request.normalization);
        let distance = storage_distance(request.distance);
        let descriptor = EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::CallerSupplied {
                contract_version: request.contract_version.clone(),
            },
            dimensions: request.dimensions,
            value_type: EmbeddingValueType::Float32,
            normalization,
            distance,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([
                (
                    "contract_version".to_owned(),
                    request.contract_version.into(),
                ),
                ("kind".to_owned(), CALLER_INPUT_KIND.into()),
            ]),
            source_projection_recipe: request
                .source_projection_recipe
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
        })?;

        let (_, projection_bytes) = self.resolve_caller_embedding_batch(
            &request.rows,
            dimensions,
            normalization,
            vector_limits,
        )?;

        self.preflight_alias(
            &request.display_name,
            descriptor.compatibility_id()?,
            request.replace_alias,
        )?;
        Ok(PreparedCallerPublication {
            display_name: request.display_name,
            replace_alias: request.replace_alias,
            descriptor,
            rows: request.rows,
            dimensions,
            normalization,
            vector_limits,
            projection_bytes,
        })
    }

    fn resolve_caller_embedding_batch(
        &self,
        rows: &[CallerEmbeddingBatchRow],
        dimensions: usize,
        normalization: EmbeddingNormalization,
        vector_limits: VectorStoreLimits,
    ) -> Result<(ValidatedEmbeddingBatch, Vec<u8>), GfError> {
        let mut eligible = BTreeSet::new();
        let mut resolved = Vec::with_capacity(rows.len());
        for row in rows {
            let uuid = self.resolve_node_selector(&row.node)?;
            let node_uuid = *uuid.as_bytes();
            eligible.insert(node_uuid);
            resolved.push(EmbeddingBatchRow {
                node_uuid,
                vector: row.vector.clone(),
            });
        }
        let batch = validate_embedding_batch(
            resolved,
            &eligible,
            dimensions,
            normalization,
            vector_limits,
            || Ok(()),
        )?;
        let projection_bytes = batch.rows().iter().flat_map(|row| row.node_uuid).collect();
        Ok((batch, projection_bytes))
    }

    fn preflight_alias(
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

    fn publish_prepared_caller_embeddings<S, C>(
        &self,
        prepared: &PreparedCallerPublication,
        mut capture_source: S,
        checkpoint: C,
    ) -> Result<EmbeddingSpaceInfo, GfError>
    where
        S: FnMut(&[u8]) -> Result<EmbeddingSourceState, SearchArtifactError>,
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let now = transaction_time_micros();
        let projection_bytes = RefCell::new(prepared.projection_bytes.clone());
        refresh_embedding_generation(
            &self.dir,
            EmbeddingRefreshRequest {
                descriptor: &prepared.descriptor,
                generated_at_micros: now,
                committed_at_micros: now,
            },
            EmbeddingRefreshLimits::default(),
            || capture_source(projection_bytes.borrow().as_slice()),
            |_| {
                let (batch, resolved_projection) = self
                    .resolve_caller_embedding_batch(
                        &prepared.rows,
                        prepared.dimensions,
                        prepared.normalization,
                        prepared.vector_limits,
                    )
                    .map_err(search_producer_error)?;
                *projection_bytes.borrow_mut() = resolved_projection;
                Ok(batch)
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

fn search_producer_error(error: GfError) -> SearchArtifactError {
    match error {
        GfError::Validation(reason) => SearchArtifactError::InvalidSelector {
            field: "caller embedding node selector",
            reason,
        },
        GfError::Storage(reason) => SearchArtifactError::SourceSnapshot { reason },
        GfError::Execution(reason) => SearchArtifactError::Build(reason),
        other => SearchArtifactError::Build(other.to_string()),
    }
}

const fn storage_normalization(value: CallerEmbeddingNormalization) -> EmbeddingNormalization {
    match value {
        CallerEmbeddingNormalization::None => EmbeddingNormalization::None,
        CallerEmbeddingNormalization::L2 => EmbeddingNormalization::L2,
    }
}

const fn storage_distance(value: CallerEmbeddingDistance) -> EmbeddingDistance {
    match value {
        CallerEmbeddingDistance::Cosine => EmbeddingDistance::Cosine,
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
    use std::collections::HashMap;

    use gf_core::uuid::Uuid;
    use gf_storage::{
        EmbeddingProducerIdentity, EmbeddingSpaceDiscoveryLimits, SearchArtifactError,
        VectorStoreLimits, discover_embedding_spaces, read_vector_snapshot,
    };

    use super::*;
    use crate::{EmbeddingSpaceProducer, PropValue};

    fn request(
        display_name: &str,
        contract_version: &str,
        dimensions: u32,
        rows: Vec<CallerEmbeddingBatchRow>,
        replace_alias: bool,
    ) -> CallerEmbeddingBatchRequest {
        CallerEmbeddingBatchRequest {
            display_name: display_name.to_owned(),
            contract_version: contract_version.to_owned(),
            dimensions,
            normalization: CallerEmbeddingNormalization::None,
            distance: CallerEmbeddingDistance::Cosine,
            source_projection_recipe: BTreeMap::from([
                ("kind".to_owned(), "explicit_nodes".to_owned()),
                ("version".to_owned(), "v1".to_owned()),
            ]),
            rows,
            replace_alias,
        }
    }

    fn row(node: NodeSelector, vector: &[f32]) -> CallerEmbeddingBatchRow {
        CallerEmbeddingBatchRow {
            node,
            vector: vector.to_vec(),
        }
    }

    fn node(graph: &GraphForge, name: &str) -> crate::NodeHandle {
        graph
            .add_node(
                "Document",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap()
    }

    fn assert_missing_alias(graph: &GraphForge, alias: &str) {
        assert!(matches!(
            graph.embedding_space(Some(alias)),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn complete_batch_is_canonical_idempotent_replaceable_and_reopenable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let alice = node(&graph, "alice");
        let bob = node(&graph, "bob");

        let first = graph
            .publish_caller_embeddings(request(
                "semantic",
                "caller-v1",
                2,
                vec![
                    row(NodeSelector::Handle(bob.clone()), &[3.0, 4.0]),
                    row(NodeSelector::Handle(alice.clone()), &[1.0, 2.0]),
                ],
                false,
            ))
            .unwrap();
        assert!(matches!(
            first.producer,
            EmbeddingSpaceProducer::CallerSupplied { ref contract_version }
                if contract_version == "caller-v1"
        ));
        let replay = graph
            .publish_caller_embeddings(request(
                "semantic",
                "caller-v1",
                2,
                vec![
                    row(NodeSelector::Handle(alice.clone()), &[1.0, 2.0]),
                    row(NodeSelector::Handle(bob.clone()), &[3.0, 4.0]),
                ],
                false,
            ))
            .unwrap();
        assert_eq!(first, replay);

        let replacement = request(
            "semantic",
            "caller-v2",
            2,
            vec![
                row(NodeSelector::Handle(alice), &[5.0, 6.0]),
                row(NodeSelector::Handle(bob), &[7.0, 8.0]),
            ],
            false,
        );
        assert!(matches!(
            graph.publish_caller_embeddings(replacement.clone()),
            Err(GfError::Validation(_))
        ));
        assert_eq!(graph.embedding_space(Some("semantic")).unwrap(), first);
        let replacement = graph
            .publish_caller_embeddings(CallerEmbeddingBatchRequest {
                replace_alias: true,
                ..replacement
            })
            .unwrap();
        assert_ne!(replacement.compatibility_id, first.compatibility_id);

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            reopened.embedding_space(Some("semantic")).unwrap(),
            replacement
        );
        let discovered = discover_embedding_spaces(
            &reopened.dir,
            EmbeddingSpaceDiscoveryLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let active = discovered
            .iter()
            .find(|space| space.compatibility_id().to_hex() == replacement.compatibility_id)
            .unwrap()
            .active()
            .unwrap();
        let rows =
            read_vector_snapshot(&active.path, 2, VectorStoreLimits::default(), || Ok(())).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].node_uuid.len(), 16);
    }

    #[test]
    fn invalid_rows_and_limits_fail_before_alias_mutation() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alice");
        let foreign_graph = GraphForge::new(None).unwrap();
        let foreign = node(&foreign_graph, "foreign");

        let cases = [
            request(
                "duplicate",
                "v1",
                2,
                vec![
                    row(NodeSelector::Handle(alice.clone()), &[1.0, 2.0]),
                    row(NodeSelector::Handle(alice.clone()), &[3.0, 4.0]),
                ],
                false,
            ),
            request(
                "missing",
                "v1",
                2,
                vec![row(NodeSelector::Uuid(Uuid::now_v7()), &[1.0, 2.0])],
                false,
            ),
            request(
                "foreign",
                "v1",
                2,
                vec![row(NodeSelector::Handle(foreign), &[1.0, 2.0])],
                false,
            ),
            request(
                "shape",
                "v1",
                2,
                vec![row(NodeSelector::Handle(alice.clone()), &[1.0])],
                false,
            ),
            request(
                "non-finite",
                "v1",
                2,
                vec![row(NodeSelector::Handle(alice.clone()), &[f32::NAN, 1.0])],
                false,
            ),
            request(
                "dimension-limit",
                "v1",
                4_097,
                vec![row(NodeSelector::Handle(alice), &vec![1.0; 4_097])],
                false,
            ),
        ];
        for request in cases {
            let alias = request.display_name.clone();
            assert!(graph.publish_caller_embeddings(request).is_err());
            assert_missing_alias(&graph, &alias);
        }
        assert!(graph.embedding_spaces().unwrap().is_empty());
    }

    #[test]
    fn empty_projection_preserves_dimension_without_primary_content() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let info = graph
            .publish_caller_embeddings(request("empty", "v1", 7, Vec::new(), false))
            .unwrap();
        assert_eq!(info.dimensions, 7);
        assert_eq!(info.active.unwrap().vector_count, 0);
        let discovered =
            discover_embedding_spaces(&graph.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })
            .unwrap();
        let active = discovered[0].active().unwrap();
        let rows =
            read_vector_snapshot(&active.path, 7, VectorStoreLimits::default(), || Ok(())).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn source_change_retries_once_and_second_change_preserves_aliases() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alice");
        let prepared = graph
            .prepare_caller_publication(request(
                "retry",
                "v1",
                2,
                vec![row(NodeSelector::Handle(alice.clone()), &[1.0, 2.0])],
                false,
            ))
            .unwrap();
        let states = [source(1, 1), source(2, 1), source(2, 1), source(2, 1)];
        let index = Cell::new(0);
        let published = graph
            .publish_prepared_caller_embeddings(
                &prepared,
                |_| {
                    let current = index.get();
                    index.set(current + 1);
                    Ok(states[current])
                },
                || Ok(()),
            )
            .unwrap();
        assert_eq!(index.get(), 4);
        assert_eq!(published.active.unwrap().source_graph_generation, 2);

        let prepared = graph
            .prepare_caller_publication(request(
                "unstable",
                "v1",
                2,
                vec![row(NodeSelector::Handle(alice), &[3.0, 4.0])],
                false,
            ))
            .unwrap();
        let states = [source(3, 1), source(4, 1), source(4, 1), source(5, 1)];
        let index = Cell::new(0);
        let error = graph
            .publish_prepared_caller_embeddings(
                &prepared,
                |_| {
                    let current = index.get();
                    index.set(current + 1);
                    Ok(states[current])
                },
                || Ok(()),
            )
            .unwrap_err();
        assert!(matches!(error, GfError::Lifecycle(_)));
        assert_missing_alias(&graph, "unstable");
    }

    #[test]
    fn property_match_is_resolved_again_for_the_retry_source() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let alice = node(&graph, "alice");
        let bob = node(&graph, "bob");
        let prepared = graph
            .prepare_caller_publication(request(
                "moving-match",
                "v1",
                2,
                vec![row(
                    NodeSelector::Match {
                        label: "Document".to_owned(),
                        property: "name".to_owned(),
                        value: PropValue::Str("alice".to_owned()),
                    },
                    &[1.0, 2.0],
                )],
                false,
            ))
            .unwrap();
        let states = [source(1, 1), source(2, 1), source(2, 1), source(2, 1)];
        let index = Cell::new(0);
        graph
            .publish_prepared_caller_embeddings(
                &prepared,
                |_| {
                    let current = index.get();
                    index.set(current + 1);
                    if current == 1 {
                        graph
                            .execute(
                                "MATCH (n:Document {name:'alice'}) SET n.name = 'former-alice'",
                            )
                            .unwrap();
                        graph
                            .execute("MATCH (n:Document {name:'bob'}) SET n.name = 'alice'")
                            .unwrap();
                    }
                    Ok(states[current])
                },
                || Ok(()),
            )
            .unwrap();

        let discovered =
            discover_embedding_spaces(&graph.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })
            .unwrap();
        let active = discovered[0].active().unwrap();
        let rows =
            read_vector_snapshot(&active.path, 2, VectorStoreLimits::default(), || Ok(())).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_uuid, *bob.uuid.as_bytes());
        assert_ne!(rows[0].node_uuid, *alice.uuid.as_bytes());
    }

    fn source(generation: u64, count: u64) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [generation as u8; 32], [9; 32], count)
    }

    #[test]
    fn caller_descriptor_cannot_impersonate_another_producer_or_expose_knowledge() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alice");
        let info = graph
            .publish_caller_embeddings(request(
                "safe",
                "v1",
                2,
                vec![row(NodeSelector::Handle(alice), &[1.0, 2.0])],
                false,
            ))
            .unwrap();
        assert!(matches!(
            info.producer,
            EmbeddingSpaceProducer::CallerSupplied { .. }
        ));
        let discovered =
            discover_embedding_spaces(&graph.dir, EmbeddingSpaceDiscoveryLimits::default(), || {
                Ok(())
            })
            .unwrap();
        assert!(matches!(
            discovered[0].descriptor().producer(),
            EmbeddingProducerIdentity::CallerSupplied { .. }
        ));
        let metadata = format!("{info:?}");
        assert!(!metadata.contains("knowledge"));
        assert!(!metadata.contains("[1.0, 2.0]"));
    }

    #[test]
    fn cancellation_remains_structured_and_does_not_bind_alias() {
        let graph = GraphForge::new(None).unwrap();
        let alice = node(&graph, "alice");
        let prepared = graph
            .prepare_caller_publication(request(
                "cancelled",
                "v1",
                2,
                vec![row(NodeSelector::Handle(alice), &[1.0, 2.0])],
                false,
            ))
            .unwrap();
        let error = graph
            .publish_prepared_caller_embeddings(
                &prepared,
                |_| Ok(source(1, 1)),
                || Err(SearchArtifactError::Cancelled),
            )
            .unwrap_err();
        assert!(matches!(error, GfError::Execution(_)));
        assert_missing_alias(&graph, "cancelled");
    }
}
