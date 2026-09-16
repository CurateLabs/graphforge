//! Rust-owned graph analysis handlers registered under the shared algorithm dispatch contract.

mod matching_partition;
use matching_partition::{
    Conductance, MaxBipartiteMatching, MaxCardinalityMatching, MaxWeightMatching, Modularity,
};
mod coloring;
use coloring::{ChromaticNumber, EdgeColoring, K1Coloring, NodeColoring};
mod paths_cycles_dag;
use paths_cycles_dag::{
    DagLongestPath, EulerConstruction, FindCycles, HasEulerCircuit, HasEulerPath, IsDag,
    MinimumKSpanningTree, SpanningTree, TopologicalSort, WeightedDagLongestPath,
};
mod structural;
use structural::{
    ArticulationPoints, Bridges, CountAutomorphisms, DyadCensus, IsPlanar, Transitivity,
    TriadCensus, TriangleCount,
};
mod embedding;
#[cfg(test)]
pub(crate) use embedding::embedding_algorithm_with_controls;
pub use embedding::{
    embedding_algorithm, embedding_algorithm_execution, embedding_algorithm_execution_with_compute,
    prepare_embedding_invocation_descriptor, prepare_embedding_invocation_descriptor_with_compute,
};

use graphforge_value::EntityTypeSelection;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, AnalyzeAlgorithm};
use graphforge_core::embedding_options::{EmbeddingAnalyzeOptions, EmbeddingOptions};
use graphforge_core::{AnalyzeOptions, GfError, OntologyMode};
use graphforge_ir::{Direction, IrLiteral};
use sha2::{Digest, Sha256};

use crate::AdjacencyProvider;
use crate::algorithm_analyze_automorphism::{AutomorphismEdge, AutomorphismGraph};
use crate::algorithm_analyze_automorphism_count::count_automorphisms;
use crate::algorithm_analyze_bipartite::{BipartiteEdge, resolve_bipartite_projection};
use crate::algorithm_analyze_bipartite_matching::maximum_bipartite_matching;
use crate::algorithm_analyze_chromatic_number::{ChromaticEdge, exact_chromatic_number};
use crate::algorithm_analyze_conductance::{ConductanceEdge, conductance};
use crate::algorithm_analyze_dag_longest_path::{DagLongestPathEdge, dag_longest_path};
use crate::algorithm_analyze_dag_longest_path_weighted::{
    WeightedDagEdge, weighted_dag_longest_path,
};
use crate::algorithm_analyze_dag_topology::stable_dag_topology;
use crate::algorithm_analyze_dyad_census::{DyadEdge, dyad_census};
use crate::algorithm_analyze_edge_coloring::{EdgeColoringEdge, greedy_edge_coloring};
use crate::algorithm_analyze_euler::{
    EulerEdge, EulerProjection, EulerTrailKind, EulerTrailOutcome,
};
use crate::algorithm_analyze_find_cycles::{CycleEdge, find_cycles};
use crate::algorithm_analyze_has_euler_circuit::{EulerCircuitEdge, has_euler_circuit};
use crate::algorithm_analyze_has_euler_path::{EulerPathEdge, has_euler_path};
use crate::algorithm_analyze_is_planar::{PlanarityEdge, is_planar};
use crate::algorithm_analyze_k1_coloring::k1_coloring;
use crate::algorithm_analyze_lowlink::low_link;
use crate::algorithm_analyze_max_cardinality_matching::{
    MatchingEdge, maximum_cardinality_matching,
};
use crate::algorithm_analyze_minimum_k_spanning_tree::minimum_k_spanning_trees;
use crate::algorithm_analyze_minimum_spanning_forest::{SpanningEdge, spanning_forest};
use crate::algorithm_analyze_modularity::{ModularityEdge, modularity, modularity_output};
use crate::algorithm_analyze_node_coloring::{NodeColoringEdge, greedy_node_coloring};
use crate::algorithm_analyze_transitivity::{TransitivityEdge, transitivity};
use crate::algorithm_analyze_triad_census::{TRIAD_NAMES, TriadEdge, triad_census};
use crate::algorithm_analyze_triangle_count::{TriangleEdge, triangle_count};
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_embedding_control::{
    EmbeddingControl, EmbeddingResourceEstimate, EmbeddingResourceLimits, FastRpResources,
    HashGnnResources, Node2VecResources, TopologyResources,
};
use crate::algorithm_embedding_fastrp::train_fastrp;
use crate::algorithm_embedding_graphsage::{
    GraphSageEdge, GraphSageNode, GraphSageProjection, preflight_graphsage_dispatch,
    train_graphsage, validate_graphsage_projection,
};
use crate::algorithm_embedding_hashgnn::{HashGnnTypeTokens, hashgnn_embeddings};
use crate::algorithm_embedding_invocation::{
    EmbeddingExecution, EmbeddingInvocationDescriptor, EmbeddingInvocationLimits,
    EmbeddingProjectionSelector, EmbeddingRngContract,
};
use crate::algorithm_embedding_node2vec::train_node2vec;
use crate::algorithm_embedding_options::{NormalizedEmbeddingOptions, normalize_embedding_options};
use crate::algorithm_embedding_output::{
    RNG_DERIVATION, RNG_VERSION, SCHEMA_VERSION, shape_embedding_output,
};
use crate::algorithm_graph::{
    AdjacencyGraph, AdjacencySelection, export_adjacency, load_node_feature_properties,
    load_node_partition_property, load_node_scalar_features,
};
use crate::algorithm_output::shape_algorithm_output;
use crate::algorithm_partition::ResolvedPartitionMap;
use crate::algorithm_weighted_undirected::{
    WeightedEdge, normalize_weighted_undirected, solve_exact_matching,
};

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "algorithm-specific canonical UUID and topology ordering",
    platforms: "Rust workspace targets",
};

pub(crate) fn register_analyze_algorithms(
    registry: &mut AlgorithmRegistry,
    directed: bool,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(ArticulationPoints))?;
    registry.register(Arc::new(Bridges))?;
    registry.register(Arc::new(ChromaticNumber))?;
    registry.register(Arc::new(CountAutomorphisms { directed }))?;
    registry.register(Arc::new(DagLongestPath))?;
    registry.register(Arc::new(DyadCensus))?;
    registry.register(Arc::new(WeightedDagLongestPath))?;
    registry.register(Arc::new(EdgeColoring))?;
    registry.register(Arc::new(EulerConstruction {
        algorithm: AnalyzeAlgorithm::EulerCircuit,
        directed,
    }))?;
    registry.register(Arc::new(EulerConstruction {
        algorithm: AnalyzeAlgorithm::EulerPath,
        directed,
    }))?;
    registry.register(Arc::new(FindCycles { directed }))?;
    registry.register(Arc::new(HasEulerCircuit { directed }))?;
    registry.register(Arc::new(HasEulerPath { directed }))?;
    registry.register(Arc::new(IsDag { directed }))?;
    registry.register(Arc::new(IsPlanar))?;
    registry.register(Arc::new(K1Coloring))?;
    registry.register(Arc::new(MaxCardinalityMatching))?;
    registry.register(Arc::new(MaxWeightMatching))?;
    registry.register(Arc::new(NodeColoring))?;
    registry.register(Arc::new(SpanningTree {
        algorithm: AnalyzeAlgorithm::MinimumSpanningTree,
        maximize: false,
    }))?;
    registry.register(Arc::new(SpanningTree {
        algorithm: AnalyzeAlgorithm::MaximumSpanningTree,
        maximize: true,
    }))?;
    registry.register(Arc::new(TriangleCount))?;
    registry.register(Arc::new(Transitivity))?;
    registry.register(Arc::new(TriadCensus))?;
    registry.register(Arc::new(TopologicalSort))
}

fn normalize_analyze_options(options: &AnalyzeOptions) -> Result<AnalyzeOptions, GfError> {
    let mut normalized = options.clone();
    if options.by == AnalyzeAlgorithm::MinimumKSpanningTree {
        let k = options.k.unwrap_or(1);
        if k == 0 {
            return Err(GfError::Validation(
                "minimum_k_spanning_tree requires k greater than zero".into(),
            ));
        }
        normalized.k = Some(k);
    } else if options.k.is_some() {
        return Err(GfError::Validation(format!(
            "{} does not accept k",
            options.by
        )));
    }
    match options.by {
        AnalyzeAlgorithm::MaxBipartiteMatching => {
            if options
                .partition_property
                .as_deref()
                .is_some_and(str::is_empty)
            {
                return Err(GfError::Validation(
                    "max_bipartite_matching requires a non-empty partition_property when supplied"
                        .into(),
                ));
            }
        }
        AnalyzeAlgorithm::Conductance => {
            if options
                .partition_property
                .as_deref()
                .is_none_or(str::is_empty)
            {
                return Err(GfError::Validation(
                    "conductance requires a non-empty partition_property".into(),
                ));
            }
        }
        AnalyzeAlgorithm::Modularity => {
            if options
                .partition_property
                .as_deref()
                .is_none_or(str::is_empty)
            {
                return Err(GfError::Validation(
                    "modularity requires a non-empty partition_property".into(),
                ));
            }
        }
        _ if options.partition_property.is_some() => {
            return Err(GfError::Validation(format!(
                "{} does not accept partition_property",
                options.by
            )));
        }
        _ => {}
    }
    Ok(normalized)
}

fn register_option_analyze_algorithm(
    registry: &mut AlgorithmRegistry,
    options: &AnalyzeOptions,
    partitions: Option<ResolvedPartitionMap>,
) -> Result<(), GfError> {
    match options.by {
        AnalyzeAlgorithm::MinimumKSpanningTree => {
            registry.register(Arc::new(MinimumKSpanningTree {
                k: options
                    .k
                    .expect("minimum-k option normalization supplies a positive k"),
            }))?;
        }
        AnalyzeAlgorithm::Conductance => {
            registry.register(Arc::new(Conductance {
                partitions: partitions.expect("conductance projection resolves partitions"),
            }))?;
        }
        AnalyzeAlgorithm::Modularity => {
            registry.register(Arc::new(Modularity {
                partitions: partitions.expect("modularity projection resolves partitions"),
            }))?;
        }
        AnalyzeAlgorithm::MaxBipartiteMatching => {
            registry.register(Arc::new(MaxBipartiteMatching { partitions }))?;
        }
        _ => {}
    }
    Ok(())
}

/// Execute a typed graph analysis algorithm through Rust dispatch and return
/// its canonical Arrow batch.
///
/// # Errors
/// Returns structured validation/execution errors for malformed selection,
/// unavailable algorithms, adjacency reads, limits, or result shaping.
pub fn analyze_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    options: &AnalyzeOptions,
) -> Result<RecordBatch, GfError> {
    analyze_algorithm_with_compute(
        provider,
        dir,
        mode,
        label,
        options,
        AlgorithmLimits::default(),
        None,
    )
}

/// Execute a typed graph analysis algorithm with caller-supplied resource limits
/// and an optional instance-owned compute pool (#570).
///
/// # Errors
/// Returns structured validation/execution errors for malformed selection,
/// unavailable algorithms, adjacency reads, limits, or result shaping.
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors analyze_algorithm plus resource-policy controls"
)]
pub fn analyze_algorithm_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    options: &AnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<RecordBatch, GfError> {
    let prepared = prepare_analyze_projection(provider, dir, mode, label, options)?;
    let algorithm = Algorithm::Analyze(prepared.options.by);
    let mut registry = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut registry, prepared.options.directed)?;
    register_option_analyze_algorithm(&mut registry, &prepared.options, prepared.partitions)?;
    let mut control = AlgorithmControl::new(limits, AlgorithmCancellation::default());
    if let Some(pool) = compute {
        control = control.with_compute_pool(pool);
    }
    let output = registry.execute(algorithm, &prepared.graph, &control)?;
    shape_algorithm_output(algorithm, &output).map_err(Into::into)
}

/// Fingerprint the exact topology, weights, and partition values consumed by analysis.
///
/// # Errors
/// Returns the same projection and option failures as [`analyze_algorithm`].
pub fn analyze_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    options: &AnalyzeOptions,
) -> Result<[u8; 32], GfError> {
    let prepared = prepare_analyze_projection(provider, dir, mode, label, options)?;
    let base = prepared.graph.descriptor_projection_fingerprint()?;
    let mut digest = Sha256::new();
    digest.update(b"graphforge_analyze_projection_v1");
    digest.update(base.as_bytes());
    if let Some(property) = prepared.options.partition_property.as_deref() {
        digest.update(
            u64::try_from(property.len())
                .map_err(|_| GfError::Execution("partition property name is too long".into()))?
                .to_be_bytes(),
        );
        digest.update(property.as_bytes());
    } else {
        digest.update(0_u64.to_be_bytes());
    }
    if let Some(partitions) = prepared.partitions {
        for (uuid, partition) in partitions.iter() {
            digest.update(uuid);
            digest.update(
                u64::try_from(partition.as_str().len())
                    .map_err(|_| GfError::Execution("partition value is too long".into()))?
                    .to_be_bytes(),
            );
            digest.update(partition.as_str().as_bytes());
        }
    }
    Ok(digest.finalize().into())
}

struct PreparedAnalyzeProjection {
    graph: AdjacencyGraph,
    options: AnalyzeOptions,
    partitions: Option<ResolvedPartitionMap>,
}

fn prepare_analyze_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    options: &AnalyzeOptions,
) -> Result<PreparedAnalyzeProjection, GfError> {
    let options = normalize_analyze_options(options)?;
    if matches!(
        options.by,
        AnalyzeAlgorithm::MinimumSpanningTree
            | AnalyzeAlgorithm::MaximumSpanningTree
            | AnalyzeAlgorithm::MinimumKSpanningTree
            | AnalyzeAlgorithm::ArticulationPoints
            | AnalyzeAlgorithm::Bridges
            | AnalyzeAlgorithm::ChromaticNumber
            | AnalyzeAlgorithm::EdgeColoring
            | AnalyzeAlgorithm::TriangleCount
            | AnalyzeAlgorithm::Transitivity
            | AnalyzeAlgorithm::IsPlanar
            | AnalyzeAlgorithm::K1Coloring
            | AnalyzeAlgorithm::NodeColoring
            | AnalyzeAlgorithm::Conductance
            | AnalyzeAlgorithm::Modularity
            | AnalyzeAlgorithm::MaxBipartiteMatching
            | AnalyzeAlgorithm::MaxCardinalityMatching
            | AnalyzeAlgorithm::MaxWeightMatching
    ) && options.directed
    {
        return Err(GfError::Validation(format!(
            "{} requires directed=false",
            options.by
        )));
    }
    if matches!(
        options.by,
        AnalyzeAlgorithm::TopologicalSort
            | AnalyzeAlgorithm::DagLongestPath
            | AnalyzeAlgorithm::DagLongestPathWeighted
            | AnalyzeAlgorithm::TriadCensus
            | AnalyzeAlgorithm::DyadCensus
    ) && !options.directed
    {
        return Err(GfError::Validation(format!(
            "{} requires directed=true",
            options.by
        )));
    }
    if !matches!(
        options.by,
        AnalyzeAlgorithm::MinimumSpanningTree
            | AnalyzeAlgorithm::MaximumSpanningTree
            | AnalyzeAlgorithm::MinimumKSpanningTree
            | AnalyzeAlgorithm::DagLongestPathWeighted
            | AnalyzeAlgorithm::Conductance
            | AnalyzeAlgorithm::Modularity
            | AnalyzeAlgorithm::MaxWeightMatching
    ) && options.weight.is_some()
    {
        return Err(GfError::Validation(format!(
            "{} does not accept an edge weight property",
            options.by
        )));
    }
    if options.by == AnalyzeAlgorithm::DagLongestPathWeighted && options.weight.is_none() {
        return Err(GfError::Validation(
            "dag_longest_path_weighted requires an edge weight property".into(),
        ));
    }
    let via = options.via.as_deref().unwrap_or("*");
    if via.is_empty() || via.trim() != via || via.chars().any(char::is_control) {
        return Err(GfError::Validation(format!(
            "invalid analyze relationship selector {via:?}"
        )));
    }
    if let Some(weight) = options.weight.as_deref()
        && (weight.is_empty() || weight.trim() != weight || weight.chars().any(char::is_control))
    {
        return Err(GfError::Validation(format!(
            "invalid analyze weight property {weight:?}"
        )));
    }
    let graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via,
            direction: if options.directed {
                Direction::Out
            } else {
                Direction::Undirected
            },
            weight: options.weight.as_deref(),
        },
    )?;
    let partitions = options
        .partition_property
        .as_deref()
        .map(|property| load_node_partition_property(&graph, dir, property))
        .transpose()?;
    Ok(PreparedAnalyzeProjection {
        graph,
        options,
        partitions,
    })
}

#[cfg(test)]
mod tests {
    use super::coloring::{
        chromatic_number_node_uuid, edge_coloring_node_uuid, k1_coloring_node_uuid,
        node_coloring_node_uuid,
    };
    use super::embedding::graphsage_projection;
    use super::matching_partition::{
        bipartite_node_uuid, cardinality_matching_node_uuid, conductance_node_uuid,
        matching_node_uuid,
    };
    use super::paths_cycles_dag::{
        dag_longest_path_node_uuid, directed_is_dag, euler_circuit_node_uuid, euler_path_node_uuid,
        find_cycles_node_uuid, spanning_node_uuid, topological_node_uuid,
        weighted_dag_longest_path_node_uuid,
    };
    use super::structural::{automorphism_allocation, bridge_node_uuid};

    use super::*;

    #[test]
    fn linear_analyzers_checkpoint_on_large_deterministic_projection() {
        let graph = AdjacencyGraph::from_resolved_projection(
            crate::algorithm_graph::ResolvedGraphProjection {
                directed: true,
                nodes: (0_u32..4_097)
                    .map(|value| {
                        let mut uuid = [0_u8; 16];
                        uuid[12..].copy_from_slice(&value.to_be_bytes());
                        uuid
                    })
                    .collect(),
                edges: Vec::new(),
            },
        )
        .unwrap();
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let analyzers: Vec<Box<dyn RustAlgorithm>> = vec![
            Box::new(TriangleCount),
            Box::new(Transitivity),
            Box::new(IsPlanar),
            Box::new(TriadCensus),
            Box::new(DyadCensus),
            Box::new(ArticulationPoints),
            Box::new(Bridges),
            Box::new(TopologicalSort),
            Box::new(IsDag { directed: true }),
            Box::new(FindCycles { directed: true }),
            Box::new(HasEulerCircuit { directed: true }),
            Box::new(HasEulerPath { directed: true }),
            Box::new(DagLongestPath),
        ];
        for analyzer in analyzers {
            analyzer.execute(&graph, &control).unwrap_or_else(|error| {
                panic!(
                    "{:?} failed on an edgeless projection: {error}",
                    analyzer.capability().algorithm
                )
            });
        }
    }

    #[test]
    fn malformed_adjacency_is_rejected_by_every_analysis_projection_adapter() {
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let missing = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0, 1],
            HashMap::from([(0, [1; 16]), (1, [2; 16])]),
            HashMap::from([(
                0,
                vec![crate::algorithm_graph::AlgorithmEdge {
                    edge_id: 1,
                    edge_uuid: [9; 16],
                    neighbor_id: 2,
                    weight: 1.0,
                }],
            )]),
        );
        let empty_partitions = ResolvedPartitionMap::try_new(
            [[1; 16]],
            [(
                [1; 16],
                crate::algorithm_partition::PartitionValue::String("a".into()),
            )],
        )
        .unwrap();
        let adapters: Vec<Box<dyn RustAlgorithm>> = vec![
            Box::new(CountAutomorphisms { directed: true }),
            Box::new(Conductance {
                partitions: empty_partitions.clone(),
            }),
            Box::new(Modularity {
                partitions: empty_partitions.clone(),
            }),
            Box::new(MaxBipartiteMatching {
                partitions: Some(empty_partitions),
            }),
            Box::new(TriangleCount),
            Box::new(Transitivity),
            Box::new(IsPlanar),
            Box::new(TriadCensus),
            Box::new(DyadCensus),
        ];
        for adapter in adapters {
            assert!(
                adapter.execute(&missing, &control).is_err(),
                "{:?}",
                adapter.capability().algorithm
            );
        }

        let edge = |neighbor_id| crate::algorithm_graph::AlgorithmEdge {
            edge_id: neighbor_id,
            edge_uuid: [7; 16],
            neighbor_id,
            weight: 1.0,
        };
        let inconsistent = AdjacencyGraph::malformed_for_defensive_tests(
            false,
            vec![0, 1],
            HashMap::from([(0, [1; 16]), (1, [2; 16])]),
            HashMap::from([(0, vec![edge(0), edge(1)])]),
        );
        let partitions = ResolvedPartitionMap::try_new(
            [[1; 16], [2; 16]],
            [
                (
                    [1; 16],
                    crate::algorithm_partition::PartitionValue::String("a".into()),
                ),
                (
                    [2; 16],
                    crate::algorithm_partition::PartitionValue::String("b".into()),
                ),
            ],
        )
        .unwrap();
        for adapter in [
            Box::new(CountAutomorphisms { directed: false }) as Box<dyn RustAlgorithm>,
            Box::new(Conductance {
                partitions: partitions.clone(),
            }),
            Box::new(Modularity {
                partitions: partitions.clone(),
            }),
            Box::new(MaxBipartiteMatching {
                partitions: Some(partitions),
            }),
        ] {
            assert!(
                adapter.execute(&inconsistent, &control).is_err(),
                "{:?}",
                adapter.capability().algorithm
            );
        }
        assert!(
            automorphism_allocation("test")
                .to_string()
                .contains("test allocation failed")
        );
    }

    #[test]
    fn missing_uuid_helpers_preserve_algorithm_specific_errors() {
        let graph = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::new(),
            HashMap::new(),
        );
        let errors = [
            spanning_node_uuid(&graph, 0, AnalyzeAlgorithm::MinimumSpanningTree).unwrap_err(),
            bipartite_node_uuid(&graph, 0).unwrap_err(),
            matching_node_uuid(&graph, 0).unwrap_err(),
            cardinality_matching_node_uuid(&graph, 0).unwrap_err(),
            conductance_node_uuid(&graph, 0).unwrap_err(),
            node_coloring_node_uuid(&graph, 0).unwrap_err(),
            k1_coloring_node_uuid(&graph, 0).unwrap_err(),
            chromatic_number_node_uuid(&graph, 0).unwrap_err(),
            topological_node_uuid(&graph, 0).unwrap_err(),
            find_cycles_node_uuid(&graph, 0).unwrap_err(),
            dag_longest_path_node_uuid(&graph, 0).unwrap_err(),
            weighted_dag_longest_path_node_uuid(&graph, 0).unwrap_err(),
            edge_coloring_node_uuid(&graph, 0).unwrap_err(),
            euler_circuit_node_uuid(&graph, 0).unwrap_err(),
            euler_path_node_uuid(&graph, 0).unwrap_err(),
            bridge_node_uuid(&graph, 0).unwrap_err(),
        ];
        for error in errors {
            assert!(error.to_string().contains("UUID identity"));
        }
    }

    #[test]
    fn graphsage_and_dag_adapters_reject_incomplete_projection_identity() {
        let mut missing_uuid = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::new(),
            HashMap::new(),
        );
        missing_uuid
            .replace_node_vectors(HashMap::from([(0, vec![1.0])]))
            .unwrap();
        assert!(
            graphsage_projection(&missing_uuid)
                .unwrap_err()
                .to_string()
                .contains("selected node has no UUID identity")
        );

        let missing_vector = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::from([(0, [1; 16])]),
            HashMap::new(),
        );
        assert!(
            graphsage_projection(&missing_vector)
                .unwrap_err()
                .to_string()
                .contains("no resolved feature vector")
        );

        let edge = crate::algorithm_graph::AlgorithmEdge {
            edge_id: 1,
            edge_uuid: [9; 16],
            neighbor_id: 1,
            weight: 1.0,
        };
        let mut missing_neighbor = AdjacencyGraph::malformed_for_defensive_tests(
            true,
            vec![0],
            HashMap::from([(0, [1; 16])]),
            HashMap::from([(0, vec![edge])]),
        );
        missing_neighbor
            .replace_node_vectors(HashMap::from([(0, vec![1.0])]))
            .unwrap();
        assert!(
            graphsage_projection(&missing_neighbor)
                .unwrap_err()
                .to_string()
                .contains("selected neighbor has no UUID identity")
        );
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        assert!(
            directed_is_dag(&missing_neighbor, &control)
                .unwrap_err()
                .to_string()
                .contains("unselected node")
        );
    }

    #[test]
    fn minimum_k_spanning_tree_normalizes_and_validates_k() {
        let defaults = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumKSpanningTree,
            directed: false,
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(defaults.k, Some(1));

        let explicit = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumKSpanningTree,
            directed: false,
            k: Some(3),
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(explicit.k, Some(3));

        assert!(matches!(
            normalize_analyze_options(&AnalyzeOptions {
                by: AnalyzeAlgorithm::MinimumKSpanningTree,
                directed: false,
                k: Some(0),
                ..AnalyzeOptions::default()
            })
            .unwrap_err(),
            GfError::Validation(message)
                if message == "minimum_k_spanning_tree requires k greater than zero"
        ));
    }

    #[test]
    fn k_is_rejected_for_other_analyze_algorithms() {
        assert!(matches!(
            normalize_analyze_options(&AnalyzeOptions {
                k: Some(2),
                ..AnalyzeOptions::default()
            })
            .unwrap_err(),
            GfError::Validation(message) if message == "is_dag does not accept k"
        ));
    }

    #[test]
    fn partition_property_validation_matches_partition_algorithms() {
        let inferred = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MaxBipartiteMatching,
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(inferred.partition_property, None);

        let explicit = normalize_analyze_options(&AnalyzeOptions {
            by: AnalyzeAlgorithm::MaxBipartiteMatching,
            partition_property: Some("side".into()),
            ..AnalyzeOptions::default()
        })
        .unwrap();
        assert_eq!(explicit.partition_property.as_deref(), Some("side"));

        for by in [
            AnalyzeAlgorithm::MaxBipartiteMatching,
            AnalyzeAlgorithm::Conductance,
            AnalyzeAlgorithm::Modularity,
        ] {
            assert!(matches!(
                normalize_analyze_options(&AnalyzeOptions {
                    by,
                    partition_property: Some(String::new()),
                    ..AnalyzeOptions::default()
                })
                .unwrap_err(),
                GfError::Validation(message) if message.contains("requires a non-empty partition_property")
            ));
        }

        assert!(
            normalize_analyze_options(&AnalyzeOptions {
                by: AnalyzeAlgorithm::Conductance,
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            })
            .is_ok()
        );
        assert!(
            normalize_analyze_options(&AnalyzeOptions {
                by: AnalyzeAlgorithm::Modularity,
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            })
            .is_ok()
        );

        assert!(matches!(
            normalize_analyze_options(&AnalyzeOptions {
                partition_property: Some("community".into()),
                ..AnalyzeOptions::default()
            })
            .unwrap_err(),
            GfError::Validation(message)
                if message == "is_dag does not accept partition_property"
        ));
    }

    pub(super) fn execute(
        graph: &AdjacencyGraph,
        algorithm: AnalyzeAlgorithm,
        directed: bool,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_analyze_algorithms(&mut registry, directed)?;
        if algorithm == AnalyzeAlgorithm::MaxBipartiteMatching {
            registry.register(Arc::new(MaxBipartiteMatching { partitions: None }))?;
        }
        registry.execute(
            Algorithm::Analyze(algorithm),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    #[test]
    fn public_projection_fingerprint_is_stable_across_provider_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let options = AnalyzeOptions {
            by: AnalyzeAlgorithm::IsDag,
            directed: true,
            ..AnalyzeOptions::default()
        };
        let first_provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let first = analyze_projection_fingerprint(
            &first_provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &options,
        )
        .unwrap();
        drop(first_provider);
        let reopened_provider =
            crate::ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let reopened = analyze_projection_fingerprint(
            &reopened_provider,
            dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &options,
        )
        .unwrap();
        assert_eq!(first, reopened);
        assert_ne!(first, [0; 32]);
    }
}
