//! Rust-owned similarity handlers registered under the shared M18 dispatch contract.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, SimilarAlgorithm};
use graphforge_core::{GfError, OntologyMode, SimilarOptions, TypeId};
use graphforge_ir::Direction;

use crate::AdjacencyProvider;
use crate::algorithm_dispatch::{
    AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmOutput, AlgorithmRegistry,
    AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_graph::{
    AdjacencyGraph, AdjacencySelection, export_adjacency, export_node_selection, load_node_vectors,
};
use crate::algorithm_output::shape_algorithm_output;
use crate::algorithm_similar_jaccard::exact_jaccard;
use crate::algorithm_similar_knn::{
    SimilarityPair, exact_cosine_knn, exact_cosine_similarity, exact_filtered_cosine_knn,
};

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "topology-ordered sources with score-descending stable target ties",
    platforms: "Rust workspace targets",
};

struct NodeSimilarity {
    k: usize,
}

struct Knn {
    k: usize,
}

struct Cosine {
    k: usize,
}

struct FilteredKnn {
    k: usize,
}

struct FilteredNodeSimilarity {
    k: usize,
}

impl RustAlgorithm for Knn {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Similar(SimilarAlgorithm::Knn),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let vectors = graph
            .node_ids()
            .iter()
            .map(|&node_id| {
                graph
                    .node_vector(node_id)
                    .ok_or_else(|| execution("validated KNN vector is missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = exact_cosine_knn(&vectors, self.k, control)?
            .into_iter()
            .map(|pair| {
                let source = graph.node_ids()[pair.source_index];
                let target = graph.node_ids()[pair.target_index];
                Ok(vec![
                    AlgorithmValue::Uuid(
                        graph
                            .node_uuid(source)
                            .ok_or_else(|| execution("selected KNN source has no UUID"))?,
                    ),
                    AlgorithmValue::Uuid(
                        graph
                            .node_uuid(target)
                            .ok_or_else(|| execution("selected KNN target has no UUID"))?,
                    ),
                    AlgorithmValue::Float64(pair.similarity),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        Ok(AlgorithmOutput {
            schema: Algorithm::Similar(SimilarAlgorithm::Knn).result_schema(),
            rows,
        })
    }
}

impl RustAlgorithm for Cosine {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Similar(SimilarAlgorithm::Cosine),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pairs = exact_cosine_similarity(&selected_vectors(graph)?, self.k, control)?;
        knn_output(graph, SimilarAlgorithm::Cosine, pairs)
    }
}

impl RustAlgorithm for FilteredKnn {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Similar(SimilarAlgorithm::FilteredKnn),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let candidates = neighbor_indices(graph, "filtered KNN")?;
        let pairs =
            exact_filtered_cosine_knn(&selected_vectors(graph)?, &candidates, self.k, control)?;
        knn_output(graph, SimilarAlgorithm::FilteredKnn, pairs)
    }
}

fn selected_vectors(graph: &AdjacencyGraph) -> Result<Vec<&[f64]>, AlgorithmError> {
    graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            graph
                .node_vector(node_id)
                .ok_or_else(|| execution("validated KNN vector is missing"))
        })
        .collect()
}

fn knn_output(
    graph: &AdjacencyGraph,
    algorithm: SimilarAlgorithm,
    pairs: Vec<SimilarityPair>,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let rows = pairs
        .into_iter()
        .map(|pair| {
            let source = graph.node_ids()[pair.source_index];
            let target = graph.node_ids()[pair.target_index];
            Ok(vec![
                AlgorithmValue::Uuid(
                    graph
                        .node_uuid(source)
                        .ok_or_else(|| execution("selected KNN source has no UUID"))?,
                ),
                AlgorithmValue::Uuid(
                    graph
                        .node_uuid(target)
                        .ok_or_else(|| execution("selected KNN target has no UUID"))?,
                ),
                AlgorithmValue::Float64(pair.similarity),
            ])
        })
        .collect::<Result<Vec<_>, AlgorithmError>>()?;
    Ok(AlgorithmOutput {
        schema: Algorithm::Similar(algorithm).result_schema(),
        rows,
    })
}

impl RustAlgorithm for NodeSimilarity {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Similar(SimilarAlgorithm::NodeSimilarity),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let neighborhoods: Vec<HashSet<u64>> = graph
            .node_ids()
            .iter()
            .map(|&node_id| {
                graph
                    .neighbors(node_id)
                    .iter()
                    .map(|edge| edge.neighbor_id)
                    .collect()
            })
            .collect();
        let rows = exact_jaccard(&neighborhoods, None, self.k, control)?
            .into_iter()
            .map(|pair| {
                let source_uuid = graph
                    .node_uuid(graph.node_ids()[pair.source_index])
                    .ok_or_else(|| execution("selected source node has no UUID identity"))?;
                let target_uuid = graph
                    .node_uuid(graph.node_ids()[pair.target_index])
                    .ok_or_else(|| execution("selected target node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(source_uuid),
                    AlgorithmValue::Uuid(target_uuid),
                    AlgorithmValue::Float64(pair.similarity),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        Ok(AlgorithmOutput {
            schema: Algorithm::Similar(SimilarAlgorithm::NodeSimilarity).result_schema(),
            rows,
        })
    }
}

impl RustAlgorithm for FilteredNodeSimilarity {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Similar(SimilarAlgorithm::FilteredNodeSimilarity),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let candidates = neighbor_indices(graph, "filtered node similarity")?;
        let pairs = exact_jaccard(&neighborhoods(graph), Some(&candidates), self.k, control)?;
        let rows = pairs
            .into_iter()
            .map(|pair| {
                let source_uuid = graph
                    .node_uuid(graph.node_ids()[pair.source_index])
                    .ok_or_else(|| execution("selected source node has no UUID identity"))?;
                let target_uuid = graph
                    .node_uuid(graph.node_ids()[pair.target_index])
                    .ok_or_else(|| execution("selected target node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(source_uuid),
                    AlgorithmValue::Uuid(target_uuid),
                    AlgorithmValue::Float64(pair.similarity),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        Ok(AlgorithmOutput {
            schema: Algorithm::Similar(SimilarAlgorithm::FilteredNodeSimilarity).result_schema(),
            rows,
        })
    }
}

fn neighborhoods(graph: &AdjacencyGraph) -> Vec<HashSet<u64>> {
    graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            graph
                .neighbors(node_id)
                .iter()
                .map(|edge| edge.neighbor_id)
                .collect()
        })
        .collect()
}

fn neighbor_indices(
    graph: &AdjacencyGraph,
    algorithm: &str,
) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    let indices = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node_id)| (node_id, index))
        .collect::<HashMap<_, _>>();
    graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            graph
                .neighbors(node_id)
                .iter()
                .map(|edge| {
                    indices.get(&edge.neighbor_id).copied().ok_or_else(|| {
                        execution(format!("{algorithm} neighbor is outside node selection"))
                    })
                })
                .collect()
        })
        .collect()
}

pub(crate) fn register_similar_algorithms(
    registry: &mut AlgorithmRegistry,
    k: usize,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(NodeSimilarity { k }))?;
    registry.register(Arc::new(Knn { k }))?;
    registry.register(Arc::new(Cosine { k }))?;
    registry.register(Arc::new(FilteredKnn { k }))?;
    registry.register(Arc::new(FilteredNodeSimilarity { k }))
}

/// Execute a typed similarity algorithm through Rust dispatch and return its
/// canonical UUID-only Arrow batch.
///
/// # Errors
/// Returns structured validation/execution errors for invalid options,
/// unavailable algorithms, adjacency reads, limits, or result shaping.
pub fn similar_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
    property_stems: &[String],
    options: SimilarOptions,
) -> Result<RecordBatch, GfError> {
    let graph = similar_projection(provider, dir, mode, label, property_stems, &options)?;
    let algorithm = Algorithm::Similar(options.by);
    let mut registry = AlgorithmRegistry::default();
    register_similar_algorithms(&mut registry, options.k)?;
    drop(options);
    let output = registry.execute(
        algorithm,
        &graph,
        &AlgorithmControl::new(
            crate::algorithm_dispatch::AlgorithmLimits::default(),
            crate::algorithm_dispatch::AlgorithmCancellation::default(),
        ),
    )?;
    shape_algorithm_output(algorithm, &output).map_err(Into::into)
}

/// Fingerprint the exact topology and vector values consumed by similarity.
///
/// # Errors
/// Returns the same projection and option failures as [`similar_algorithm`].
pub fn similar_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
    property_stems: &[String],
    options: &SimilarOptions,
) -> Result<[u8; 32], GfError> {
    similar_projection(provider, dir, mode, label, property_stems, options)
        .and_then(|graph| graph.descriptor_projection_fingerprint())
        .map(|fingerprint| *fingerprint.as_bytes())
}

fn similar_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
    property_stems: &[String],
    options: &SimilarOptions,
) -> Result<AdjacencyGraph, GfError> {
    let by = options.by;
    let k = options.k;
    let vector_property = options.vector_property.as_deref();
    let via = options.via.as_deref();
    if let Some(property) = vector_property
        && (property.is_empty()
            || property.trim() != property
            || property.chars().any(char::is_control))
    {
        return Err(GfError::Validation(format!(
            "invalid similar vector property {property:?}"
        )));
    }
    match (by, vector_property) {
        (SimilarAlgorithm::NodeSimilarity | SimilarAlgorithm::FilteredNodeSimilarity, Some(_)) => {
            return Err(GfError::Validation(format!(
                "similar.{} does not accept vector_property",
                by.as_str()
            )));
        }
        (
            SimilarAlgorithm::Knn | SimilarAlgorithm::FilteredKnn | SimilarAlgorithm::Cosine,
            None,
        ) => {
            return Err(GfError::Validation(format!(
                "similar.{} requires vector_property",
                by.as_str()
            )));
        }
        _ => {}
    }
    if matches!(by, SimilarAlgorithm::Knn | SimilarAlgorithm::Cosine) && via.is_some() {
        return Err(GfError::Validation(format!(
            "similar.{} does not accept via",
            by.as_str()
        )));
    }
    let via = via.unwrap_or("*");
    if via.is_empty() || via.trim() != via || via.chars().any(char::is_control) {
        return Err(GfError::Validation(format!(
            "invalid similar relationship selector {via:?}"
        )));
    }
    if k == 0 {
        return Err(GfError::Validation("similar k must be positive".into()));
    }
    let uses_vectors = matches!(
        by,
        SimilarAlgorithm::Knn | SimilarAlgorithm::FilteredKnn | SimilarAlgorithm::Cosine
    );
    let mut graph = if matches!(by, SimilarAlgorithm::Knn | SimilarAlgorithm::Cosine) {
        export_node_selection(dir, Some(label))?
    } else {
        export_adjacency(
            provider,
            dir,
            mode,
            AdjacencySelection {
                label: Some(label),
                via,
                direction: Direction::Out,
                weight: None,
            },
        )?
    };
    if let Some(property) = vector_property.filter(|_| uses_vectors) {
        load_node_vectors(&mut graph, dir, property_stems, property)?;
        validate_cosine_vectors(&graph, property, by)?;
    }
    Ok(graph)
}

fn validate_cosine_vectors(
    graph: &AdjacencyGraph,
    property: &str,
    algorithm: SimilarAlgorithm,
) -> Result<(), GfError> {
    for &node_id in graph.node_ids() {
        let vector = graph
            .node_vector(node_id)
            .ok_or_else(|| GfError::Execution("validated KNN vector is missing".into()))?;
        let norm_squared = vector.iter().try_fold(0.0, |sum, value| {
            let next = sum + value * value;
            next.is_finite().then_some(next).ok_or_else(|| {
                GfError::Validation(format!(
                    "similar.{} vector property {property:?} has infinite norm",
                    algorithm.as_str()
                ))
            })
        })?;
        if norm_squared == 0.0 {
            return Err(GfError::Validation(format!(
                "similar.{} vector property {property:?} has zero norm",
                algorithm.as_str()
            )));
        }
    }
    Ok(())
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits, AlgorithmValue};

    fn execute(
        graph: &AdjacencyGraph,
        k: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Similar(SimilarAlgorithm::NodeSimilarity);
        let mut registry = AlgorithmRegistry::default();
        register_similar_algorithms(&mut registry, k)?;
        registry.execute(
            algorithm,
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn uuid(id: u64) -> AlgorithmValue {
        AlgorithmValue::Uuid(u128::from(id).to_be_bytes())
    }

    #[test]
    fn node_similarity_returns_stable_reciprocal_top_k_jaccard_rows() {
        let mut registry = AlgorithmRegistry::default();
        register_similar_algorithms(&mut registry, 2).unwrap();
        for algorithm in [
            SimilarAlgorithm::Knn,
            SimilarAlgorithm::Cosine,
            SimilarAlgorithm::FilteredKnn,
            SimilarAlgorithm::FilteredNodeSimilarity,
        ] {
            assert_eq!(
                registry
                    .capabilities()
                    .iter()
                    .filter(|capability| capability.algorithm == Algorithm::Similar(algorithm))
                    .count(),
                1
            );
        }
        let graph =
            AdjacencyGraph::with_test_edges(5, &[(0, 3), (0, 4), (0, 4), (1, 3), (1, 4), (2, 3)]);
        let output = execute(
            &graph,
            2,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();

        assert_eq!(
            output.rows,
            vec![
                vec![uuid(0), uuid(1), AlgorithmValue::Float64(1.0)],
                vec![uuid(0), uuid(2), AlgorithmValue::Float64(0.5)],
                vec![uuid(1), uuid(0), AlgorithmValue::Float64(1.0)],
                vec![uuid(1), uuid(2), AlgorithmValue::Float64(0.5)],
                vec![uuid(2), uuid(0), AlgorithmValue::Float64(0.5)],
                vec![uuid(2), uuid(1), AlgorithmValue::Float64(0.5)],
            ]
        );
        assert_eq!(
            output.schema,
            Algorithm::Similar(SimilarAlgorithm::NodeSimilarity).result_schema()
        );

        let top_one = execute(
            &graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            top_one.rows,
            vec![
                vec![uuid(0), uuid(1), AlgorithmValue::Float64(1.0)],
                vec![uuid(1), uuid(0), AlgorithmValue::Float64(1.0)],
                vec![uuid(2), uuid(0), AlgorithmValue::Float64(0.5)],
            ]
        );
    }

    #[test]
    fn node_similarity_handles_multigraph_boundaries_and_shared_controls() {
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 0), (0, 0), (1, 0)]);
        assert_eq!(
            execute(
                &multigraph,
                10,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows,
            vec![
                vec![uuid(0), uuid(1), AlgorithmValue::Float64(1.0)],
                vec![uuid(1), uuid(0), AlgorithmValue::Float64(1.0)],
            ]
        );
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 2), (1, 3)]);
        assert!(
            execute(
                &disconnected,
                10,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows
            .is_empty()
        );
        assert!(
            execute(
                &AdjacencyGraph::default(),
                10,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows
            .is_empty()
        );
        assert!(matches!(
            execute(
                &AdjacencyGraph::with_test_counts(3, 0),
                10,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2
            })
        ));
        assert!(matches!(
            execute(
                &multigraph,
                10,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1
            })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute(&multigraph, 10, AlgorithmLimits::default(), cancellation,),
            Err(AlgorithmError::Cancelled)
        );
        let no_iterations = AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        };
        let sparse = AdjacencyGraph::with_test_edges(3, &[(0, 0)]);
        assert_eq!(
            execute(&sparse, 10, no_iterations, AlgorithmCancellation::default()),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );

        let mut registry = AlgorithmRegistry::default();
        register_similar_algorithms(&mut registry, 10).unwrap();
        assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
    }
}
