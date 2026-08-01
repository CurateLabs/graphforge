//! Exact graph-native queries over one verified complete embedding generation.

use std::path::Path;

use graphforge_storage::{
    EmbeddingReadDecision, SearchArtifactError, VectorSearchHit, exact_cosine_search,
    read_vector_snapshot, validate_vector,
};

use crate::PreparedEmbeddingRead;
use crate::vector_lifecycle::{
    VectorLifecycleLimits, capture_topology_snapshot, project_label_members, validate_result_limit,
};

/// Statically distinct query vectors accepted by complete embedding generations.
#[derive(Clone, Copy, Debug)]
pub enum EmbeddingVectorQuery<'a> {
    /// A caller-supplied finite, non-zero vector.
    Raw(&'a [f32]),
    /// Reuse this graph node's vector from the selected complete generation.
    Node([u8; 16]),
}

/// Complete backend request after public alias and label resolution.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingGenerationQuery<'a> {
    /// Freshness-checked complete generation selected by the caller.
    pub prepared: &'a PreparedEmbeddingRead,
    /// Local catalog identity used only for current graph membership projection.
    pub label_id: u32,
    /// Raw or existing-node query form.
    pub query: EmbeddingVectorQuery<'a>,
    /// Maximum exact-cosine hits.
    pub limit: usize,
}

/// Search one verified complete embedding generation with current graph membership.
///
/// Mildly stale and explicitly forced-stale prepared reads may serve; an
/// ordinary substantially stale read cannot. The immutable generation is read
/// again on the single bounded topology retry so cancellation or mutation
/// never returns partial or mismatched hits.
///
/// # Errors
/// Returns structured freshness, selector, dimension, corruption, membership,
/// resource, cancellation, source, or repeated-concurrent-mutation errors.
pub fn search_embedding_generation<C>(
    project_dir: &Path,
    request: EmbeddingGenerationQuery<'_>,
    limits: VectorLifecycleLimits,
    mut checkpoint: C,
) -> Result<Vec<VectorSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_prepared_read(request.prepared)?;
    validate_result_limit(request.limit, limits.vector)?;
    if let EmbeddingVectorQuery::Raw(vector) = request.query {
        validate_vector(vector, limits.vector)?;
    }

    let publication = request.prepared.publication();
    let dimension = usize::try_from(publication.descriptor.dimensions()).map_err(|_| {
        invalid(
            "embedding dimension",
            "cannot be represented on this platform",
        )
    })?;

    for attempt in 1_u8..=2 {
        let before = capture_topology_snapshot(project_dir, limits, &mut checkpoint)?;
        let eligible =
            project_label_members(project_dir, request.label_id, limits, &mut checkpoint)?;
        let projected = capture_topology_snapshot(project_dir, limits, &mut checkpoint)?;
        if before != projected {
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation);
            }
            continue;
        }

        let rows =
            read_vector_snapshot(&publication.path, dimension, limits.vector, &mut checkpoint)?;
        let query = match request.query {
            EmbeddingVectorQuery::Raw(vector) => vector,
            EmbeddingVectorQuery::Node(node_uuid) => {
                if !eligible.contains(&node_uuid) {
                    return Err(invalid(
                        "similar_to",
                        "node does not belong to the requested label",
                    ));
                }
                let index = rows
                    .binary_search_by_key(&node_uuid, |row| row.node_uuid)
                    .map_err(|_| {
                        invalid(
                            "similar_to",
                            "node has no vector in the selected complete generation",
                        )
                    })?;
                rows[index].vector.as_slice()
            }
        };
        if query.len() != dimension {
            return Err(invalid(
                "vector",
                format!(
                    "dimension {} does not match embedding-space dimension {dimension}",
                    query.len()
                ),
            ));
        }
        let hits = exact_cosine_search(
            &rows,
            query,
            &eligible,
            request.limit,
            limits.vector,
            &mut checkpoint,
        )?;
        let after = capture_topology_snapshot(project_dir, limits, &mut checkpoint)?;
        if before == after {
            return Ok(hits);
        }
        if attempt == 2 {
            return Err(SearchArtifactError::ConcurrentMutation);
        }
    }
    unreachable!("the bounded embedding search loop returns on both terminal paths")
}

fn validate_prepared_read(prepared: &PreparedEmbeddingRead) -> Result<(), SearchArtifactError> {
    match prepared.decision() {
        EmbeddingReadDecision::ServeFresh
        | EmbeddingReadDecision::ServeStale { .. }
        | EmbeddingReadDecision::ServeForcedStale { .. } => Ok(()),
        EmbeddingReadDecision::RefreshRequired { reason } => Err(SearchArtifactError::Stale {
            reason: format!(
                "embedding space is substantially stale: {}",
                reason.as_str()
            ),
        }),
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::{BTreeMap, BTreeSet, HashSet};

    use graphforge_core::uuid::Uuid;
    use graphforge_ir::{OntologyMode, TypeId};
    use graphforge_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityInput,
        EmbeddingDistance, EmbeddingMutationJournalLimits, EmbeddingNormalization,
        EmbeddingProducerIdentity, EmbeddingPublicationRequest, EmbeddingSourceState,
        EmbeddingValueType, GraphWriter, SearchCoordinationLimits, ValidatedEmbeddingBatch,
        delete_nodes, generation::bump_search_generation, generation::read_search_generation,
        publish_embedding_generation, reset_embedding_mutation_journal, validate_embedding_batch,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{EmbeddingReadLimits, prepare_embedding_read};

    const LABEL_ID: u32 = 9;

    fn uuid(value: u8) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        bytes
    }

    fn descriptor() -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "test-adapter".to_owned(),
                model: "test-model".to_owned(),
                revision: "r1".to_owned(),
                contract_version: "v1".to_owned(),
            },
            dimensions: 2,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".to_owned(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), "Document".into())]),
        })
        .unwrap()
    }

    fn write_members(dir: &TempDir, values: &[u8]) {
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        for &value in values {
            writer
                .create_node(Uuid::from_bytes(uuid(value)), TypeId(LABEL_ID))
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn validated_batch(rows: &[(u8, [f32; 2])]) -> ValidatedEmbeddingBatch {
        let eligible = rows
            .iter()
            .map(|(value, _)| uuid(*value))
            .collect::<BTreeSet<_>>();
        validate_embedding_batch(
            rows.iter()
                .map(|(value, vector)| EmbeddingBatchRow {
                    node_uuid: uuid(*value),
                    vector: vector.to_vec(),
                })
                .collect(),
            &eligible,
            2,
            EmbeddingNormalization::None,
            Default::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn prepared(
        dir: &TempDir,
        descriptor: &EmbeddingCompatibilityDescriptor,
        batch: &ValidatedEmbeddingBatch,
    ) -> PreparedEmbeddingRead {
        let source = EmbeddingSourceState::new(
            read_search_generation(dir.path()).unwrap(),
            [3; 32],
            [4; 32],
            u64::try_from(batch.rows().len()).unwrap(),
        );
        let publication = publish_embedding_generation(
            dir.path(),
            EmbeddingPublicationRequest {
                descriptor,
                source,
                batch,
                generated_at_micros: 10,
                committed_at_micros: 11,
            },
            Default::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .publication()
        .clone();
        reset_embedding_mutation_journal(
            dir.path(),
            &publication.manifest,
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        prepare_embedding_read(
            dir.path(),
            descriptor,
            source,
            false,
            EmbeddingReadLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap()
    }

    fn request<'a>(
        prepared: &'a PreparedEmbeddingRead,
        query: EmbeddingVectorQuery<'a>,
        limit: usize,
    ) -> EmbeddingGenerationQuery<'a> {
        EmbeddingGenerationQuery {
            prepared,
            label_id: LABEL_ID,
            query,
            limit,
        }
    }

    #[test]
    fn raw_and_existing_node_queries_share_stable_exact_ordering() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[2, 1]);
        let descriptor = descriptor();
        let batch = validated_batch(&[(2, [1.0, 0.0]), (1, [1.0, 0.0])]);
        let prepared = prepared(&dir, &descriptor, &batch);

        let raw = search_embedding_generation(
            dir.path(),
            request(&prepared, EmbeddingVectorQuery::Raw(&[1.0, 0.0]), 2),
            Default::default(),
            || Ok(()),
        )
        .unwrap();
        let node = search_embedding_generation(
            dir.path(),
            request(&prepared, EmbeddingVectorQuery::Node(uuid(1)), 2),
            Default::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(raw, node);
        assert_eq!(
            raw.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            [uuid(1), uuid(2)]
        );
        assert!(raw.iter().all(|hit| hit.score == 1.0));
    }

    #[test]
    fn filters_orphans_and_rejects_missing_or_ineligible_query_nodes() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[1, 2, 3]);
        let descriptor = descriptor();
        let batch = validated_batch(&[(1, [1.0, 0.0]), (2, [0.0, 1.0])]);
        let prepared = prepared(&dir, &descriptor, &batch);
        assert_eq!(
            delete_nodes(dir.path(), &HashSet::from([uuid(2)])).unwrap(),
            1
        );

        let hits = search_embedding_generation(
            dir.path(),
            request(&prepared, EmbeddingVectorQuery::Raw(&[0.0, 1.0]), 3),
            Default::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            [uuid(1)]
        );
        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&prepared, EmbeddingVectorQuery::Node(uuid(2)), 1),
                Default::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::InvalidSelector {
                field: "similar_to",
                ..
            })
        ));
        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&prepared, EmbeddingVectorQuery::Node(uuid(3)), 1),
                Default::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::InvalidSelector {
                field: "similar_to",
                ..
            })
        ));
    }

    #[test]
    fn validates_dimensions_limits_and_cancellation_without_partial_hits() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[1]);
        let descriptor = descriptor();
        let batch = validated_batch(&[(1, [1.0, 0.0])]);
        let prepared = prepared(&dir, &descriptor, &batch);

        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&prepared, EmbeddingVectorQuery::Raw(&[1.0]), 1),
                Default::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::InvalidSelector {
                field: "vector",
                ..
            })
        ));
        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&prepared, EmbeddingVectorQuery::Raw(&[1.0, 0.0]), 0),
                Default::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::InvalidSelector { field: "limit", .. })
        ));
        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&prepared, EmbeddingVectorQuery::Raw(&[1.0, 0.0]), 1),
                Default::default(),
                || Err(SearchArtifactError::Cancelled),
            ),
            Err(SearchArtifactError::Cancelled)
        ));

        std::fs::write(
            prepared
                .publication()
                .path
                .join(graphforge_storage::VECTOR_DATA_FILE),
            b"not parquet",
        )
        .unwrap();
        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&prepared, EmbeddingVectorQuery::Raw(&[1.0, 0.0]), 1),
                Default::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn ordinary_substantially_stale_preparation_cannot_serve() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[1]);
        let descriptor = descriptor();
        let batch = validated_batch(&[(1, [1.0, 0.0])]);
        let fresh = prepared(&dir, &descriptor, &batch);
        let recorded = fresh.publication().manifest.source();
        bump_search_generation(dir.path()).unwrap();
        let current = EmbeddingSourceState::new(
            recorded.graph_generation() + 1,
            recorded.label_membership_digest(),
            recorded.dependency_input_digest(),
            recorded.eligible_uuid_count(),
        );
        let stale = prepare_embedding_read(
            dir.path(),
            &descriptor,
            current,
            false,
            EmbeddingReadLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            search_embedding_generation(
                dir.path(),
                request(&stale, EmbeddingVectorQuery::Raw(&[1.0, 0.0]), 1),
                Default::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::Stale { .. })
        ));
    }

    #[test]
    fn retries_once_then_fails_closed_on_repeated_graph_mutation() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[1]);
        let descriptor = descriptor();
        let batch = validated_batch(&[(1, [1.0, 0.0])]);
        let prepared = prepared(&dir, &descriptor, &batch);
        let checks = Cell::new(0_u8);

        let result = search_embedding_generation(
            dir.path(),
            request(&prepared, EmbeddingVectorQuery::Raw(&[1.0, 0.0]), 1),
            Default::default(),
            || {
                checks.set(checks.get().saturating_add(1));
                bump_search_generation(dir.path()).unwrap();
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(SearchArtifactError::ConcurrentMutation)
        ));
        assert!(checks.get() > 2);
    }
}
