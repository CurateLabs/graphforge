//! Synthetic reserved-sidecar isolation harness for the #772 acceptance gate.
//!
//! The files written below are deliberately test-only sentinels. They are not
//! an epistemic schema and must not become a production read contract (#782).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    AssertionGraphRefInput, CapabilityId, CreateAssertionRequest, EmbeddingAnalyzeOptions,
    EmbeddingOptions, EnableCapabilityRequest, FastRpOptions, GraphForge, GraphSageOptions,
    HashGnnOptions, ListAssertionsRequest, Node2VecOptions, OperationId, SearchIndexOptions,
    WriteContext, validate_embedding_options,
};
use graphforge_core::algorithms::{
    Algorithm, AlgorithmFieldType, AnalyzeAlgorithm, ClusterAlgorithm, PathAlgorithm,
    RankAlgorithm, SimilarAlgorithm,
};
use graphforge_core::{
    AnalyzeOptions, ClusterOptions, PathsOptions, PropValue, RankOptions, SimilarOptions,
};
use graphforge_core::{FindOptions, NodeSelector};
use graphforge_exec::ExecutionResult;
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome,
};
use tempfile::TempDir;
use uuid::Uuid;

const EPHEMERAL_QUERY_METADATA: [&str; 1] = ["graphforge.query_id"];
const FORBIDDEN_KNOWLEDGE_NAMES: [&str; 15] = [
    "provenance",
    "confidence",
    "assertion",
    "evidence",
    "belief",
    "epistemic",
    "hypothesis",
    "valid_time",
    "from",
    "to",
    "valid_from",
    "valid_to",
    "as_of",
    "algorithm_run_uuid",
    "run_uuid",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CatalogDisposition {
    ExecutablePairedCase,
    ExpectedUnavailable,
}

#[derive(Clone, Copy, Debug)]
struct EmbeddingIsolationCase {
    by: AnalyzeAlgorithm,
    algorithm_version: &'static str,
    disposition: CatalogDisposition,
}

impl EmbeddingIsolationCase {
    fn algorithm(self) -> Algorithm {
        Algorithm::Analyze(self.by)
    }

    fn invocation(self) -> EmbeddingAnalyzeOptions {
        let options = match self.by {
            AnalyzeAlgorithm::Node2Vec => EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 4,
                walk_length: 3,
                walks_per_node: 2,
                window_size: 1,
                negative_samples: 1,
                epochs: 1,
                seed: 7,
                ..Node2VecOptions::default()
            }),
            AnalyzeAlgorithm::GraphSage => {
                let options = GraphSageOptions {
                    dimensions: 2,
                    hidden_dimensions: 2,
                    layers: 1,
                    sample_sizes: vec![1],
                    epochs: 1,
                    negative_samples: 1,
                    learning_rate: 0.001,
                    feature_properties: vec!["features".into()],
                    seed: 13,
                    ..GraphSageOptions::default()
                };
                EmbeddingOptions::GraphSage(options)
            }
            AnalyzeAlgorithm::FastRandomProjection => {
                EmbeddingOptions::FastRandomProjection(FastRpOptions::default())
            }
            AnalyzeAlgorithm::HashGnn => EmbeddingOptions::HashGnn(HashGnnOptions::default()),
            by => panic!("{by:?} is not an embedding algorithm"),
        };
        EmbeddingAnalyzeOptions {
            by: self.by,
            via: Some("KNOWS".into()),
            directed: self.by != AnalyzeAlgorithm::GraphSage,
            weight: matches!(
                self.by,
                AnalyzeAlgorithm::Node2Vec | AnalyzeAlgorithm::FastRandomProjection
            )
            .then(|| "weight".into()),
            options,
        }
    }
}

const EMBEDDING_CASES: [EmbeddingIsolationCase; 4] = [
    EmbeddingIsolationCase {
        by: AnalyzeAlgorithm::Node2Vec,
        algorithm_version: "node2vec-v1",
        disposition: CatalogDisposition::ExecutablePairedCase,
    },
    EmbeddingIsolationCase {
        by: AnalyzeAlgorithm::GraphSage,
        algorithm_version: "graphsage-unsupervised-v1",
        disposition: CatalogDisposition::ExecutablePairedCase,
    },
    EmbeddingIsolationCase {
        by: AnalyzeAlgorithm::FastRandomProjection,
        algorithm_version: "fastrp-v1",
        disposition: CatalogDisposition::ExecutablePairedCase,
    },
    EmbeddingIsolationCase {
        by: AnalyzeAlgorithm::HashGnn,
        algorithm_version: "hashgnn-v1",
        disposition: CatalogDisposition::ExecutablePairedCase,
    },
];

#[derive(Clone, Copy, Debug)]
enum PairedAlgorithmCase {
    Rank(RankAlgorithm),
    Cluster {
        by: ClusterAlgorithm,
        vector_property: Option<&'static str>,
    },
    Similar {
        by: SimilarAlgorithm,
        vector_property: Option<&'static str>,
    },
    Paths(PathAlgorithm),
    SourceFreePath(PathAlgorithm),
    Steiner {
        by: PathAlgorithm,
        prize_property: Option<&'static str>,
    },
    Analyze(AnalyzeAlgorithm),
}

const RANK_CLUSTER_CASES: &[PairedAlgorithmCase] = &[
    PairedAlgorithmCase::Rank(RankAlgorithm::PageRank),
    PairedAlgorithmCase::Rank(RankAlgorithm::Betweenness),
    PairedAlgorithmCase::Rank(RankAlgorithm::Closeness),
    PairedAlgorithmCase::Rank(RankAlgorithm::HarmonicCloseness),
    PairedAlgorithmCase::Rank(RankAlgorithm::Degree),
    PairedAlgorithmCase::Rank(RankAlgorithm::Eigenvector),
    PairedAlgorithmCase::Rank(RankAlgorithm::ArticleRank),
    PairedAlgorithmCase::Rank(RankAlgorithm::HitsHub),
    PairedAlgorithmCase::Rank(RankAlgorithm::HitsAuthority),
    PairedAlgorithmCase::Rank(RankAlgorithm::Celf),
    PairedAlgorithmCase::Rank(RankAlgorithm::ClusteringCoefficient),
    PairedAlgorithmCase::Rank(RankAlgorithm::Triangles),
    PairedAlgorithmCase::Rank(RankAlgorithm::KCore),
    PairedAlgorithmCase::Rank(RankAlgorithm::PreferentialAttachment),
    PairedAlgorithmCase::Rank(RankAlgorithm::AdamicAdar),
    PairedAlgorithmCase::Rank(RankAlgorithm::CommonNeighbors),
    PairedAlgorithmCase::Rank(RankAlgorithm::ResourceAllocation),
    PairedAlgorithmCase::Rank(RankAlgorithm::TotalNeighbors),
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Louvain,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Leiden,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::LabelPropagation,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::SpeakerListener,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::GirvanNewman,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::ModularityOptimization,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::FastGreedy,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::InfoMap,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::LeadingEigenvector,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Walktrap,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Spinglass,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Hdbscan,
        vector_property: Some("features"),
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::KMeans,
        vector_property: Some("features"),
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::ApproximateMaxKCut,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Components,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::StronglyConnected,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::Biconnected,
        vector_property: None,
    },
    PairedAlgorithmCase::Cluster {
        by: ClusterAlgorithm::KCoreDecomposition,
        vector_property: None,
    },
];

const REMAINING_CASES: &[PairedAlgorithmCase] = &[
    PairedAlgorithmCase::Similar {
        by: SimilarAlgorithm::NodeSimilarity,
        vector_property: None,
    },
    PairedAlgorithmCase::Similar {
        by: SimilarAlgorithm::FilteredNodeSimilarity,
        vector_property: None,
    },
    PairedAlgorithmCase::Similar {
        by: SimilarAlgorithm::Knn,
        vector_property: Some("features"),
    },
    PairedAlgorithmCase::Similar {
        by: SimilarAlgorithm::Cosine,
        vector_property: Some("features"),
    },
    PairedAlgorithmCase::Similar {
        by: SimilarAlgorithm::FilteredKnn,
        vector_property: Some("features"),
    },
    PairedAlgorithmCase::Paths(PathAlgorithm::Bfs),
    PairedAlgorithmCase::Paths(PathAlgorithm::Dijkstra),
    PairedAlgorithmCase::Paths(PathAlgorithm::DijkstraAllPairs),
    PairedAlgorithmCase::Paths(PathAlgorithm::AStar),
    PairedAlgorithmCase::Paths(PathAlgorithm::BellmanFord),
    PairedAlgorithmCase::Paths(PathAlgorithm::FloydWarshall),
    PairedAlgorithmCase::Paths(PathAlgorithm::DeltaStepping),
    PairedAlgorithmCase::Paths(PathAlgorithm::Yens),
    PairedAlgorithmCase::Paths(PathAlgorithm::Dfs),
    PairedAlgorithmCase::Paths(PathAlgorithm::TransitiveClosure),
    PairedAlgorithmCase::Paths(PathAlgorithm::MaxFlow),
    PairedAlgorithmCase::Paths(PathAlgorithm::MaxFlowEdges),
    PairedAlgorithmCase::Paths(PathAlgorithm::MinCut),
    PairedAlgorithmCase::Paths(PathAlgorithm::MinCutEdges),
    PairedAlgorithmCase::Paths(PathAlgorithm::MinCostMaxFlow),
    PairedAlgorithmCase::Paths(PathAlgorithm::MinCostMaxFlowEdges),
    PairedAlgorithmCase::Paths(PathAlgorithm::RandomWalk),
    PairedAlgorithmCase::SourceFreePath(PathAlgorithm::GomoryHuTree),
    PairedAlgorithmCase::Steiner {
        by: PathAlgorithm::MinSteinerTree,
        prize_property: None,
    },
    PairedAlgorithmCase::Steiner {
        by: PathAlgorithm::PrizeCollectingSteinerTree,
        prize_property: Some("prize"),
    },
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::ArticulationPoints),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::Bridges),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::ChromaticNumber),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::CountAutomorphisms),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::MinimumSpanningTree),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::MaximumSpanningTree),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::TriangleCount),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::TriadCensus),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::Transitivity),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::IsPlanar),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::NodeColoring),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::K1Coloring),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::FindCycles),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::DagLongestPath),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::DagLongestPathWeighted),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::EdgeColoring),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::EulerCircuit),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::EulerPath),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::HasEulerCircuit),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::HasEulerPath),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::TopologicalSort),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::IsDag),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::DyadCensus),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::Conductance),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::Modularity),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching),
    PairedAlgorithmCase::Analyze(AnalyzeAlgorithm::MaxWeightMatching),
];

impl PairedAlgorithmCase {
    const fn algorithm(self) -> Algorithm {
        match self {
            Self::Rank(by) => Algorithm::Rank(by),
            Self::Cluster { by, .. } => Algorithm::Cluster(by),
            Self::Similar { by, .. } => Algorithm::Similar(by),
            Self::Paths(by) | Self::SourceFreePath(by) | Self::Steiner { by, .. } => {
                Algorithm::Paths(by)
            }
            Self::Analyze(by) => Algorithm::Analyze(by),
        }
    }

    fn execute(
        self,
        graph: &GraphForge,
        source: &NodeSelector,
        target: &NodeSelector,
    ) -> Result<RecordBatch, graphforge_core::GfError> {
        match self {
            Self::Rank(by) => graph.rank(
                "Person",
                RankOptions {
                    by,
                    ..RankOptions::default()
                },
            ),
            Self::Cluster {
                by,
                vector_property,
            } => graph.cluster(
                "Person",
                ClusterOptions {
                    by,
                    vector_property: vector_property.map(str::to_owned),
                    ..ClusterOptions::default()
                },
            ),
            Self::Similar {
                by,
                vector_property,
            } => graph.similar(
                "Person",
                SimilarOptions {
                    by,
                    k: 3,
                    vector_property: vector_property.map(str::to_owned),
                    ..SimilarOptions::default()
                },
            ),
            Self::Paths(by) => execute_standard_path(graph, by, source, target),
            Self::SourceFreePath(by) => graph.paths(
                None,
                None,
                PathsOptions {
                    by,
                    directed: false,
                    via: Some("KNOWS".into()),
                    weight: Some("weight".into()),
                    ..PathsOptions::default()
                },
            ),
            Self::Steiner { by, prize_property } => graph.paths(
                None,
                None,
                steiner_options(by, fixture_terminal_uuids(graph), prize_property),
            ),
            Self::Analyze(by) => graph.analyze(
                Some(match by {
                    AnalyzeAlgorithm::MinimumKSpanningTree => "Connected",
                    AnalyzeAlgorithm::EulerCircuit | AnalyzeAlgorithm::EulerPath => "EulerEmpty",
                    _ => "Person",
                }),
                AnalyzeOptions {
                    by,
                    via: (by == AnalyzeAlgorithm::MaxBipartiteMatching)
                        .then(|| "BIPARTITE".to_owned()),
                    directed: !matches!(
                        by,
                        AnalyzeAlgorithm::ArticulationPoints
                            | AnalyzeAlgorithm::Bridges
                            | AnalyzeAlgorithm::ChromaticNumber
                            | AnalyzeAlgorithm::MinimumSpanningTree
                            | AnalyzeAlgorithm::MinimumKSpanningTree
                            | AnalyzeAlgorithm::MaximumSpanningTree
                            | AnalyzeAlgorithm::EdgeColoring
                            | AnalyzeAlgorithm::TriangleCount
                            | AnalyzeAlgorithm::Transitivity
                            | AnalyzeAlgorithm::IsPlanar
                            | AnalyzeAlgorithm::NodeColoring
                            | AnalyzeAlgorithm::K1Coloring
                            | AnalyzeAlgorithm::Conductance
                            | AnalyzeAlgorithm::Modularity
                            | AnalyzeAlgorithm::MaxBipartiteMatching
                            | AnalyzeAlgorithm::MaxCardinalityMatching
                            | AnalyzeAlgorithm::MaxWeightMatching
                    ),
                    weight: matches!(
                        by,
                        AnalyzeAlgorithm::MinimumSpanningTree
                            | AnalyzeAlgorithm::MinimumKSpanningTree
                            | AnalyzeAlgorithm::MaximumSpanningTree
                            | AnalyzeAlgorithm::MaxWeightMatching
                            | AnalyzeAlgorithm::DagLongestPathWeighted
                            | AnalyzeAlgorithm::Conductance
                            | AnalyzeAlgorithm::Modularity
                    )
                    .then(|| "weight".to_owned()),
                    partition_property: matches!(
                        by,
                        AnalyzeAlgorithm::Conductance
                            | AnalyzeAlgorithm::Modularity
                            | AnalyzeAlgorithm::MaxBipartiteMatching
                    )
                    .then(|| "side".to_owned()),
                    ..AnalyzeOptions::default()
                },
            ),
        }
    }
}

fn execute_standard_path(
    graph: &GraphForge,
    by: PathAlgorithm,
    source: &NodeSelector,
    target: &NodeSelector,
) -> Result<RecordBatch, graphforge_core::GfError> {
    let target = (!matches!(
        by,
        PathAlgorithm::DijkstraAllPairs
            | PathAlgorithm::FloydWarshall
            | PathAlgorithm::Dfs
            | PathAlgorithm::TransitiveClosure
            | PathAlgorithm::RandomWalk
    ))
    .then_some(target);
    graph.paths(
        source,
        target,
        PathsOptions {
            by,
            k: usize::from(by == PathAlgorithm::Yens) + 1,
            weight: (!matches!(
                by,
                PathAlgorithm::Bfs
                    | PathAlgorithm::Dfs
                    | PathAlgorithm::TransitiveClosure
                    | PathAlgorithm::MinCostMaxFlow
                    | PathAlgorithm::MinCostMaxFlowEdges
            ))
            .then(|| "weight".to_owned()),
            capacity_property: None,
            cost_property: matches!(
                by,
                PathAlgorithm::MinCostMaxFlow | PathAlgorithm::MinCostMaxFlowEdges
            )
            .then(|| "weight".to_owned()),
            heuristic: (by == PathAlgorithm::AStar).then(|| "heuristic".to_owned()),
            ..PathsOptions::default()
        },
    )
}

struct IsolationFixture {
    _root: TempDir,
    bare_dir: PathBuf,
    enriched_dir: PathBuf,
    bare_data_dir: PathBuf,
    enriched_data_dir: PathBuf,
}

impl IsolationFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("fixture root");
        let seed = root.path().join("seed");
        let bare_dir = root.path().join("bare");
        let enriched_dir = root.path().join("enriched");
        fs::create_dir(&seed).expect("create seed project");

        {
            let graph = GraphForge::new(Some(seed.to_str().expect("UTF-8 seed path")))
                .expect("open seed graph");
            graph
                .execute(
                    "CREATE (a:Person:Connected {name:'Alice', features:[1.0], heuristic:3.0, side:'left', prize:8.0}), \
                     (b:Person:Connected {name:'Bob', features:[1.1], heuristic:2.0, side:'right', prize:6.0}), \
                     (c:Person:Connected {name:'Carol', features:[1.2], heuristic:1.0, side:'left', prize:4.0}), \
                     (d:Person:Connected {name:'Dave', features:[1.3], heuristic:0.0, side:'right', prize:7.0}), \
                     (e:Person:Connected {name:'Eve', features:[1.4], heuristic:0.0, side:'left', prize:2.0}), \
                     (f:Person:Connected {name:'Frank', features:[1.5], heuristic:0.0, side:'right', prize:1.0}), \
                     (g:Person {name:'Grace', features:[11.0], heuristic:0.0, side:'left', prize:0.0}), \
                     (h:Person {name:'Heidi', features:[11.1], heuristic:0.0, side:'right', prize:0.0}), \
                     (i:Person {name:'Ivan', features:[11.2], heuristic:0.0, side:'left', prize:0.0}), \
                     (j:Person {name:'Judy', features:[11.3], heuristic:0.0, side:'right', prize:0.0}), \
                     (m:Person {name:'Mallory', features:[11.4], heuristic:0.0, side:'left', prize:0.0}), \
                     (n:Person {name:'Niaj', features:[101.0], heuristic:0.0, side:'right', prize:0.0}), \
                     (a)-[:KNOWS {weight:1}]->(b), \
                     (b)-[:KNOWS {weight:2}]->(c), \
                     (a)-[:KNOWS {weight:4}]->(c), \
                     (c)-[:KNOWS {weight:3}]->(d), \
                     (d)-[:KNOWS {weight:1}]->(e), \
                     (e)-[:KNOWS {weight:1}]->(f), \
                     (g)-[:BIPARTITE {weight:1}]->(h), \
                     (h)-[:BIPARTITE {weight:1}]->(i), \
                     (i)-[:BIPARTITE {weight:1}]->(j), \
                     (j)-[:BIPARTITE {weight:1}]->(m)",
                )
                .expect("seed deterministic graph");
        }

        copy_tree(&seed, &bare_dir);
        copy_tree(&seed, &enriched_dir);
        assert_eq!(
            snapshot_tree(&bare_dir),
            snapshot_tree(&enriched_dir),
            "clones must begin byte-identical"
        );
        let bare_data_dir = graphforge_storage::resolve_project_generation(&bare_dir)
            .expect("resolve bare")
            .participants_root();
        let enriched_data_dir = graphforge_storage::resolve_project_generation(&enriched_dir)
            .expect("resolve enriched")
            .participants_root();
        let generation = graphforge_storage::generation::read_topology_generation(&bare_data_dir)
            .expect("bare generation");

        remove_if_present(&bare_data_dir.join("provenance"));
        remove_if_present(&bare_data_dir.join("knowledge"));
        inject_synthetic_knowledge(&enriched_data_dir);

        assert_graph_native_state_equal(&bare_data_dir, &enriched_data_dir);
        assert_eq!(
            graphforge_storage::generation::read_topology_generation(&bare_data_dir)
                .expect("bare generation after divergence"),
            generation
        );
        assert_eq!(
            graphforge_storage::generation::read_topology_generation(&enriched_data_dir)
                .expect("enriched generation after divergence"),
            generation
        );

        Self {
            _root: root,
            bare_dir,
            enriched_dir,
            bare_data_dir,
            enriched_data_dir,
        }
    }

    fn open(&self) -> (GraphForge, GraphForge) {
        (
            GraphForge::new(Some(self.bare_dir.to_str().expect("UTF-8 bare path")))
                .expect("open bare graph"),
            GraphForge::new(Some(
                self.enriched_dir.to_str().expect("UTF-8 enriched path"),
            ))
            .expect("open enriched graph"),
        )
    }
}

#[test]
fn reserved_sidecars_do_not_affect_default_reads_or_traversal() {
    let fixture = IsolationFixture::new();
    let (bare, enriched) = fixture.open();

    for query in [
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        "MATCH (:Person {name:'Alice'})-[:KNOWS*1..3]->(n:Person) \
         RETURN n.name AS name ORDER BY name",
    ] {
        let bare_result = bare.execute(query);
        let enriched_result = enriched.execute(query);
        assert_query_parity(query, bare_result, enriched_result);
    }
}

#[test]
fn find_is_identical_across_absent_populated_corrupt_and_future_knowledge() {
    let (_fixture, absent, populated, corrupt, future) = persistent_find_fixtures();
    for root in [&populated, &corrupt, &future] {
        populate_find_knowledge(root);
    }

    let corrupt_generation = graphforge_storage::resolve_project_generation(&corrupt)
        .expect("resolve populated corrupt fixture");
    let corrupt_assertions = corrupt_generation
        .participant_path("knowledge", "assertions")
        .expect("knowledge assertion participant");
    drop(corrupt_generation);
    fs::write(&corrupt_assertions, b"deliberately corrupt knowledge bytes")
        .expect("corrupt only the knowledge participant");

    publish_future_knowledge_generation(&future);

    let expected = find_alice(&absent);
    for (state, root) in [
        ("populated", populated.as_path()),
        ("corrupt", corrupt.as_path()),
        ("future", future.as_path()),
    ] {
        let actual = find_alice(root);
        assert_eq!(actual.schema(), expected.schema(), "{state} schema");
        assert_eq!(actual.columns(), expected.columns(), "{state} values");
    }

    let corrupt_graph =
        GraphForge::new(Some(corrupt.to_str().expect("UTF-8 path"))).expect("reopen corrupt");
    let assertion_error = corrupt_graph
        .list_assertions(ListAssertionsRequest::default())
        .expect_err("the deliberately corrupt knowledge table must fail when requested");
    assert_eq!(assertion_error.code(), "GF_PROJECT_CORRUPT");

    let future_graph =
        GraphForge::new(Some(future.to_str().expect("UTF-8 path"))).expect("reopen future");
    let capabilities = future_graph
        .project_capabilities()
        .expect("inspect future capability without opening participants");
    let support = capabilities.batches[0]
        .column_by_name("support")
        .expect("support column")
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("support strings");
    assert!(
        support
            .iter()
            .flatten()
            .any(|value| value == "unsupported_future")
    );
}

fn persistent_find_fixtures() -> (TempDir, PathBuf, PathBuf, PathBuf, PathBuf) {
    let fixture = tempfile::tempdir().expect("persistent find fixture");
    let seed = fixture.path().join("seed");
    fs::create_dir(&seed).expect("create find seed");
    let graph =
        GraphForge::new(Some(seed.to_str().expect("UTF-8 path"))).expect("open persistent graph");
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".to_owned(), PropValue::Str("Alice".to_owned()))]),
        )
        .expect("create Alice");
    drop(graph);

    let absent = fixture.path().join("absent");
    let populated = fixture.path().join("populated");
    let corrupt = fixture.path().join("corrupt");
    let future = fixture.path().join("future");
    for destination in [&absent, &populated, &corrupt, &future] {
        copy_tree(&seed, destination);
    }
    (fixture, absent, populated, corrupt, future)
}

fn populate_find_knowledge(root: &Path) {
    let graph =
        GraphForge::new(Some(root.to_str().expect("UTF-8 path"))).expect("open knowledge clone");
    for (capability_id, operation_uuid) in [
        (CapabilityId::Provenance, Uuid::now_v7()),
        (CapabilityId::Knowledge, Uuid::now_v7()),
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(operation_uuid),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .expect("enable knowledge capability");
    }
    let found = graph
        .find(FindOptions {
            query: Some("Alice".to_owned()),
            label: Some("Person".to_owned()),
            ..FindOptions::default()
        })
        .expect("resolve Alice UUID");
    let uuid_bytes: [u8; 16] = found
        .column_by_name("node_uuid")
        .expect("node_uuid")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("UUID array")
        .value(0)
        .try_into()
        .expect("16-byte UUID");
    graph
        .create_assertion(CreateAssertionRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            assertion_uuid: Uuid::now_v7(),
            claim: "Alice exists".to_owned(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: Uuid::from_bytes(uuid_bytes),
                graph_kind: graphforge_api::GraphObjectKind::Node,
                role: graphforge_api::AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .expect("populate immutable knowledge");
}

fn find_alice(root: &Path) -> RecordBatch {
    GraphForge::new(Some(root.to_str().expect("UTF-8 path")))
        .expect("reopen find fixture")
        .find(FindOptions {
            query: Some("Alice".to_owned()),
            label: Some("Person".to_owned()),
            ..FindOptions::default()
        })
        .expect("find must remain knowledge-neutral")
}

fn publish_future_knowledge_generation(root: &Path) {
    let parent =
        graphforge_storage::resolve_project_generation(root).expect("resolve future parent");
    let parent_uuid = parent.generation_uuid();
    let capabilities = parent
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id.clone(),
            capability_version: if capability.capability_id == "knowledge" {
                99
            } else {
                capability.capability_version
            },
        })
        .collect();
    let participants = parent
        .participant_snapshots()
        .expect("copy complete parent inventory")
        .into_iter()
        .map(|snapshot| ProjectParticipant {
            capability_version: if snapshot.capability_id == "knowledge" {
                99
            } else {
                snapshot.capability_version
            },
            capability_id: snapshot.capability_id,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "parquet" => ProjectParticipantEncoding::Parquet,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "json" => ProjectParticipantEncoding::Json,
                encoding => panic!("unexpected participant encoding {encoding}"),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect();
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        graphforge_storage::stage_project_generation(root, &request)
            .expect("stage future knowledge")
    else {
        panic!("fresh future publication replayed");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), parent_uuid);
                Ok(())
            },
        )
        .expect("validate complete future generation")
        .publish()
        .expect("publish future knowledge generation");
}

#[test]
fn search_publication_rejects_unmanifested_sidecars_without_mutation() {
    let fixture = IsolationFixture::new();
    let reserved_before = snapshot_reserved_sidecars(&fixture.enriched_data_dir);
    assert!(
        !reserved_before.is_empty(),
        "enriched clone needs sentinels"
    );

    let (bare, enriched) = fixture.open();
    bare.index_search(
        "Person",
        SearchIndexOptions::Text {
            properties: None,
            rebuild: false,
        },
    )
    .expect("valid project can publish a graph-native text index");
    let error = enriched
        .index_search(
            "Person",
            SearchIndexOptions::Text {
                properties: None,
                rebuild: false,
            },
        )
        .expect_err("unmanifested participant bytes must fail closed");
    assert_eq!(error.code(), "GF_TRANSACTION_FAILED");
    assert_query_parity(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        bare.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name"),
        enriched.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name"),
    );

    assert_eq!(
        snapshot_reserved_sidecars(&fixture.enriched_data_dir),
        reserved_before,
        "failed publication must not rewrite unmanifested sidecars"
    );
}

#[test]
fn typed_catalog_partition_is_unique_exhaustive_and_probes_unavailable_handlers() {
    let fixture = IsolationFixture::new();
    let (bare, enriched) = fixture.open();
    let partition = catalog_partition();
    let catalog = complete_catalog();

    assert_eq!(
        partition.len(),
        catalog.len(),
        "catalog omission or duplicate"
    );
    assert_eq!(
        partition.keys().copied().collect::<HashSet<_>>(),
        catalog.into_iter().collect(),
        "partition must cover every canonical typed catalog value exactly once"
    );
    assert_eq!(
        partition
            .values()
            .filter(|value| **value == CatalogDisposition::ExecutablePairedCase)
            .count(),
        RANK_CLUSTER_CASES.len()
            + REMAINING_CASES.len()
            + EMBEDDING_CASES
                .iter()
                .filter(|case| case.disposition == CatalogDisposition::ExecutablePairedCase)
                .count(),
        "every executable handler must own one paired isolation case"
    );
    let paired_algorithms = RANK_CLUSTER_CASES
        .iter()
        .chain(REMAINING_CASES)
        .copied()
        .map(PairedAlgorithmCase::algorithm)
        .chain(
            EMBEDDING_CASES
                .iter()
                .filter(|case| case.disposition == CatalogDisposition::ExecutablePairedCase)
                .map(|case| case.algorithm()),
        )
        .collect::<HashSet<_>>();
    assert_eq!(
        paired_algorithms.len(),
        RANK_CLUSTER_CASES.len()
            + REMAINING_CASES.len()
            + EMBEDDING_CASES
                .iter()
                .filter(|case| case.disposition == CatalogDisposition::ExecutablePairedCase)
                .count(),
        "paired cases must be unique"
    );
    assert_eq!(
        paired_algorithms,
        partition
            .iter()
            .filter_map(|(algorithm, disposition)| {
                (*disposition == CatalogDisposition::ExecutablePairedCase).then_some(*algorithm)
            })
            .collect(),
        "every executable handler owns one paired case"
    );
    assert_embedding_foundation_contract(&partition);
    let source = NodeSelector::Match {
        label: "Person".into(),
        property: "name".into(),
        value: PropValue::Str("Alice".into()),
    };
    let target = NodeSelector::Match {
        label: "Person".into(),
        property: "name".into(),
        value: PropValue::Str("Dave".into()),
    };
    for case in RANK_CLUSTER_CASES.iter().chain(REMAINING_CASES) {
        let algorithm = case.algorithm();
        assert_algorithm_parity(
            algorithm,
            case.execute(&bare, &source, &target),
            case.execute(&enriched, &source, &target),
            false,
        );
    }
    for case in EMBEDDING_CASES
        .iter()
        .filter(|case| case.disposition == CatalogDisposition::ExecutablePairedCase)
    {
        let invocation = case.invocation();
        assert_algorithm_parity(
            case.algorithm(),
            bare.analyze_embedding(Some("Person"), &invocation),
            enriched.analyze_embedding(Some("Person"), &invocation),
            false,
        );
    }

    for (algorithm, disposition) in partition {
        if disposition != CatalogDisposition::ExpectedUnavailable {
            continue;
        }
        let (left, right) = match algorithm {
            Algorithm::Paths(by) => (
                execute_unavailable_path(&bare, by, &source, &target),
                execute_unavailable_path(&enriched, by, &source, &target),
            ),
            Algorithm::Analyze(by) => {
                let options = AnalyzeOptions {
                    by,
                    partition_property: (by == AnalyzeAlgorithm::Conductance)
                        .then(|| "partition".into()),
                    ..AnalyzeOptions::default()
                };
                (
                    bare.analyze(None, options.clone()),
                    enriched.analyze(None, options),
                )
            }
            _ => panic!("all rank, cluster, and similar handlers are registered"),
        };
        assert_algorithm_parity(algorithm, left, right, true);
    }
}

fn assert_embedding_foundation_contract(partition: &HashMap<Algorithm, CatalogDisposition>) {
    let expected_algorithms = [
        AnalyzeAlgorithm::Node2Vec,
        AnalyzeAlgorithm::GraphSage,
        AnalyzeAlgorithm::FastRandomProjection,
        AnalyzeAlgorithm::HashGnn,
    ];
    assert_eq!(
        EMBEDDING_CASES.map(|case| case.by),
        expected_algorithms,
        "every embedding leaf must have an explicit promotable isolation case"
    );

    for case in EMBEDDING_CASES {
        let algorithm = case.algorithm();
        assert_eq!(
            partition.get(&algorithm),
            Some(&case.disposition),
            "{algorithm:?} disposition must be explicit"
        );

        let invocation = case.invocation();
        validate_embedding_options(&invocation)
            .unwrap_or_else(|error| panic!("{algorithm:?} typed options: {error}"));
        assert_no_forbidden_embedding_option_names(&invocation, algorithm);

        let result_schema = algorithm.result_schema();
        assert!(!result_schema.includes_node_properties, "{algorithm:?}");
        assert_eq!(
            result_schema
                .fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", AlgorithmFieldType::Uuid, false),
                ("embedding", AlgorithmFieldType::Float32List, false),
            ],
            "{algorithm:?} logical result schema"
        );
        for field in result_schema.fields {
            assert_not_forbidden_public_name(field.name, algorithm, "result column");
        }

        let metadata = [
            ("graphforge.algorithm", algorithm.as_str()),
            ("graphforge.verb", "analyze"),
            ("graphforge.algorithm_version", case.algorithm_version),
            ("graphforge.algorithm_schema_version", "1"),
            ("graphforge.dimensions", "<typed dimensions>"),
            ("graphforge.seed", "<typed seed>"),
            ("graphforge.rng_version", "splitmix64-v1"),
            (
                "graphforge.rng_derivation",
                "graphforge-embedding-substream-v1",
            ),
        ];
        assert_eq!(metadata.len(), 8, "{algorithm:?} metadata contract");
        for (key, _) in metadata {
            assert_not_forbidden_public_name(key, algorithm, "metadata key");
        }

        let mut forbidden = invocation;
        forbidden.via = Some("evidence".into());
        let error = validate_embedding_options(&forbidden)
            .expect_err("knowledge-layer selectors must fail at the Rust-owned boundary");
        assert!(
            error
                .to_string()
                .contains("cannot select knowledge-layer field evidence"),
            "{algorithm:?} returned unexpected selector error: {error}"
        );
    }
}

fn assert_no_forbidden_embedding_option_names(
    options: &EmbeddingAnalyzeOptions,
    algorithm: Algorithm,
) {
    let debug = format!("{options:?}").to_ascii_lowercase();
    for forbidden in [
        "as_of",
        "confidence",
        "provenance",
        "assertion",
        "evidence",
        "belief",
        "hypothesis",
        "valid_time",
        "algorithm_run_uuid",
        "run_uuid",
    ] {
        assert!(
            !debug.contains(forbidden),
            "{algorithm:?} exposes forbidden option term {forbidden:?}"
        );
    }
}

fn assert_not_forbidden_public_name(name: &str, algorithm: Algorithm, surface: &str) {
    let normalized = name.to_ascii_lowercase().replace(['-', '.'], "_");
    assert!(
        !FORBIDDEN_KNOWLEDGE_NAMES.iter().any(|forbidden| {
            normalized == *forbidden
                || (!matches!(*forbidden, "from" | "to") && normalized.contains(forbidden))
        }),
        "{algorithm:?} exposes forbidden {surface} {name:?}"
    );
}

#[test]
fn steiner_algorithms_ignore_reserved_sidecars_and_normalize_uuid_projections() {
    let fixture = IsolationFixture::new();
    let (bare, enriched) = fixture.open();
    let mut explicit = fixture_named_uuids(&bare, &["Alice", "Dave"]);
    explicit.sort_unstable();
    explicit.dedup();
    let direct = vec![explicit[1], explicit[0], explicit[1]];

    for (by, prize_property) in [
        (PathAlgorithm::MinSteinerTree, None),
        (PathAlgorithm::PrizeCollectingSteinerTree, Some("prize")),
    ] {
        let algorithm = Algorithm::Paths(by);
        let options = steiner_options(by, direct.clone(), prize_property);
        let normalized_options = steiner_options(by, explicit.clone(), prize_property);
        assert_no_forbidden_option_names(&options, algorithm);

        let bare_direct = bare
            .paths(None, None, options.clone())
            .unwrap_or_else(|error| panic!("{algorithm:?} direct bare execution: {error}"));
        let bare_explicit = bare
            .paths(None, None, normalized_options.clone())
            .unwrap_or_else(|error| panic!("{algorithm:?} explicit bare execution: {error}"));
        let enriched_direct = enriched
            .paths(None, None, options)
            .unwrap_or_else(|error| panic!("{algorithm:?} direct enriched execution: {error}"));
        let enriched_explicit = enriched
            .paths(None, None, normalized_options)
            .unwrap_or_else(|error| panic!("{algorithm:?} explicit enriched execution: {error}"));

        assert_exact_steiner_result(&bare_direct, algorithm);
        for (description, result) in [
            ("normalized explicit UUID projection", &bare_explicit),
            ("knowledge-enriched direct projection", &enriched_direct),
            (
                "knowledge-enriched explicit UUID projection",
                &enriched_explicit,
            ),
        ] {
            assert_eq!(
                bare_direct.schema(),
                result.schema(),
                "{algorithm:?} {description} schema and metadata"
            );
            assert_eq!(
                bare_direct.num_rows(),
                result.num_rows(),
                "{algorithm:?} {description} rows"
            );
            assert_eq!(
                bare_direct.columns(),
                result.columns(),
                "{algorithm:?} {description} UUIDs, weights, and order"
            );
        }
    }
}

#[test]
fn gomory_hu_ignores_reserved_sidecars_and_preserves_explicit_uuid_projection() {
    let fixture = IsolationFixture::new();
    let (bare, enriched) = fixture.open();
    let algorithm = Algorithm::Paths(PathAlgorithm::GomoryHuTree);
    let options = PathsOptions {
        by: PathAlgorithm::GomoryHuTree,
        directed: false,
        via: Some("KNOWS".into()),
        weight: Some("weight".into()),
        ..PathsOptions::default()
    };
    assert_no_forbidden_option_names(&options, algorithm);

    let bare_direct = bare
        .paths(None, None, options.clone())
        .expect("bare graph-native Gomory-Hu projection");
    let bare_replay = bare
        .paths(None, None, options.clone())
        .expect("bare deterministic Gomory-Hu replay");
    let enriched_direct = enriched
        .paths(None, None, options.clone())
        .expect("knowledge-enriched Gomory-Hu projection");
    let enriched_replay = enriched
        .paths(None, None, options)
        .expect("knowledge-enriched deterministic Gomory-Hu replay");

    for result in [
        &bare_direct,
        &bare_replay,
        &enriched_direct,
        &enriched_replay,
    ] {
        assert_exact_gomory_hu_result(result, algorithm);
    }
    assert_eq!(bare_direct, bare_replay);
    assert_eq!(bare_direct, enriched_direct);
    assert_eq!(bare_direct, enriched_replay);

    let mut explicit_projection =
        fixture_named_uuids(&bare, &["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]);
    explicit_projection.sort_unstable();
    let sources = bare_direct
        .column_by_name("source_uuid")
        .expect("Gomory-Hu source UUIDs")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("fixed-size source UUIDs");
    let targets = bare_direct
        .column_by_name("target_uuid")
        .expect("Gomory-Hu target UUIDs")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("fixed-size target UUIDs");
    let mut result_projection = (0..bare_direct.num_rows())
        .flat_map(|row| [sources.value(row), targets.value(row)])
        .map(|value| value.try_into().expect("16-byte Gomory-Hu UUID"))
        .collect::<Vec<[u8; 16]>>();
    result_projection.sort_unstable();
    result_projection.dedup();
    assert_eq!(
        result_projection, explicit_projection,
        "graph-native forest must preserve the equivalent explicit UUID projection"
    );
}

fn assert_exact_gomory_hu_result(result: &RecordBatch, algorithm: Algorithm) {
    assert_eq!(result.num_rows(), 5, "six connected nodes form five rows");
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", &DataType::FixedSizeBinary(16), false),
            ("target_uuid", &DataType::FixedSizeBinary(16), false),
            ("cut_value", &DataType::Float64, false),
        ]
    );
    let schema = result.schema();
    assert_eq!(schema.metadata().len(), 3);
    assert_eq!(schema.metadata()["graphforge.verb"], "paths");
    assert_eq!(schema.metadata()["graphforge.algorithm"], "gomory_hu_tree");
    assert_eq!(
        schema.metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_no_forbidden_knowledge_names(schema.as_ref(), algorithm);

    let sources = result
        .column_by_name("source_uuid")
        .expect("source UUID column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("fixed-size source UUIDs");
    let targets = result
        .column_by_name("target_uuid")
        .expect("target UUID column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("fixed-size target UUIDs");
    let cuts = result
        .column_by_name("cut_value")
        .expect("cut value column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 cut values");
    assert!((0..result.num_rows()).all(|row| {
        !sources.is_null(row)
            && !targets.is_null(row)
            && !cuts.is_null(row)
            && sources.value(row) < targets.value(row)
            && cuts.value(row).is_finite()
            && cuts.value(row) >= 0.0
    }));
    assert!((0..result.num_rows() - 1).all(|row| {
        (sources.value(row), targets.value(row)) < (sources.value(row + 1), targets.value(row + 1))
    }));
}

#[test]
fn automorphism_count_ignores_reserved_sidecars_and_equivalent_resolved_projections() {
    let fixture = IsolationFixture::new();
    let (bare, enriched) = fixture.open();
    let algorithm = Algorithm::Analyze(AnalyzeAlgorithm::CountAutomorphisms);
    let options = AnalyzeOptions {
        by: AnalyzeAlgorithm::CountAutomorphisms,
        directed: true,
        via: Some("KNOWS".into()),
        ..AnalyzeOptions::default()
    };
    let option_debug = format!("{options:?}").to_ascii_lowercase();
    for forbidden in [
        "as_of",
        "confidence",
        "provenance",
        "assertion",
        "evidence",
        "belief",
        "hypothesis",
        "valid_time",
        "algorithm_run_uuid",
        "run_uuid",
    ] {
        assert!(
            !option_debug.contains(forbidden),
            "{algorithm:?} exposes forbidden option term {forbidden:?}"
        );
    }

    let bare_direct = bare
        .analyze(None, options.clone())
        .expect("bare all-node projection");
    let bare_resolved = bare
        .analyze(Some("Person"), options.clone())
        .expect("bare resolved label projection");
    let enriched_direct = enriched
        .analyze(None, options.clone())
        .expect("enriched all-node projection");
    let enriched_resolved = enriched
        .analyze(Some("Person"), options)
        .expect("enriched resolved label projection");

    for result in [
        &bare_direct,
        &bare_resolved,
        &enriched_direct,
        &enriched_resolved,
    ] {
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [("count", &DataType::UInt64, false)]
        );
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.column(0).null_count(), 0);
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "count_automorphisms"
        );
        assert_eq!(result.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_no_forbidden_knowledge_names(result.schema().as_ref(), algorithm);
    }
    assert_eq!(bare_direct, bare_resolved);
    assert_eq!(bare_direct, enriched_direct);
    assert_eq!(bare_direct, enriched_resolved);
}

fn steiner_options(
    by: PathAlgorithm,
    terminal_uuids: Vec<[u8; 16]>,
    prize_property: Option<&str>,
) -> PathsOptions {
    PathsOptions {
        by,
        directed: false,
        via: Some("KNOWS".into()),
        weight: Some("weight".into()),
        terminal_uuids,
        prize_property: prize_property.map(str::to_owned),
        ..PathsOptions::default()
    }
}

fn assert_exact_steiner_result(result: &RecordBatch, algorithm: Algorithm) {
    assert!(result.num_rows() > 0, "{algorithm:?} must select edges");
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("edge_uuid", &DataType::FixedSizeBinary(16), false),
            ("source_uuid", &DataType::FixedSizeBinary(16), false),
            ("target_uuid", &DataType::FixedSizeBinary(16), false),
            ("weight", &DataType::Float64, false),
        ]
    );
    let schema = result.schema();
    let metadata = schema.metadata();
    assert_eq!(metadata.len(), 3, "{algorithm:?} metadata must stay stable");
    assert_eq!(metadata["graphforge.verb"], "paths");
    assert_eq!(metadata["graphforge.algorithm"], algorithm.as_str());
    assert_eq!(metadata["graphforge.algorithm_schema_version"], "1");
    for name in ["edge_uuid", "source_uuid", "target_uuid"] {
        let column = result
            .column_by_name(name)
            .expect("canonical UUID column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("fixed-size UUID column");
        assert_eq!(column.len(), result.num_rows());
        assert!((0..column.len()).all(|row| !column.is_null(row)));
    }
    let weights = result
        .column_by_name("weight")
        .expect("canonical weight column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 weights");
    assert!((0..weights.len()).all(|row| !weights.is_null(row) && weights.value(row).is_finite()));
    assert_no_forbidden_knowledge_names(result.schema().as_ref(), algorithm);
}

fn assert_no_forbidden_option_names(options: &PathsOptions, algorithm: Algorithm) {
    let debug = format!("{options:?}").to_ascii_lowercase();
    for forbidden in [
        "as_of",
        "confidence",
        "provenance",
        "assertion",
        "evidence",
        "belief",
        "hypothesis",
        "valid_time",
        "algorithm_run_uuid",
    ] {
        assert!(
            !debug.contains(forbidden),
            "{algorithm:?} exposes forbidden option term {forbidden:?}"
        );
    }
}

fn execute_unavailable_path(
    graph: &GraphForge,
    by: PathAlgorithm,
    source: &NodeSelector,
    target: &NodeSelector,
) -> Result<RecordBatch, graphforge_core::GfError> {
    let steiner = matches!(
        by,
        PathAlgorithm::MinSteinerTree | PathAlgorithm::PrizeCollectingSteinerTree
    );
    let options = PathsOptions {
        by,
        directed: !steiner,
        terminal_uuids: if steiner {
            fixture_terminal_uuids(graph)
        } else {
            Vec::new()
        },
        prize_property: (by == PathAlgorithm::PrizeCollectingSteinerTree).then(|| "prize".into()),
        ..PathsOptions::default()
    };
    if steiner {
        graph.paths(None, None, options)
    } else {
        let target = (by != PathAlgorithm::RandomWalk).then_some(target);
        graph.paths(source, target, options)
    }
}

fn fixture_terminal_uuids(graph: &GraphForge) -> Vec<[u8; 16]> {
    let result = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY n.name LIMIT 2")
        .expect("resolve graph-native Steiner terminals");
    let column = result.batches[0]
        .column_by_name("node_uuid")
        .expect("terminal UUID column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("fixed-size terminal UUIDs");
    (0..column.len())
        .map(|row| column.value(row).try_into().expect("16-byte terminal UUID"))
        .collect()
}

fn fixture_named_uuids(graph: &GraphForge, names: &[&str]) -> Vec<[u8; 16]> {
    names
        .iter()
        .map(|name| {
            let result = graph
                .execute(&format!(
                    "MATCH (n:Person {{name:'{name}'}}) RETURN n.node_uuid AS node_uuid"
                ))
                .expect("resolve graph-native Steiner terminal");
            let column = result.batches[0]
                .column_by_name("node_uuid")
                .expect("terminal UUID column")
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("fixed-size terminal UUIDs");
            assert_eq!(column.len(), 1, "fixture terminal {name:?}");
            column.value(0).try_into().expect("16-byte terminal UUID")
        })
        .collect()
}

#[test]
fn min_cost_views_exclude_knowledge_and_run_metadata_on_synthetic_enriched_clone() {
    // The enriched clone is only an isolation sentinel, not a Bazel-migration0/epistemic read contract.
    let fixture = IsolationFixture::new();
    for reserved_root in ["knowledge", "provenance"] {
        assert!(
            !fixture.bare_data_dir.join(reserved_root).exists(),
            "bare clone must omit reserved {reserved_root} sentinel"
        );
        assert!(
            fixture.enriched_data_dir.join(reserved_root).exists(),
            "enriched clone must contain synthetic test-only {reserved_root} sentinel"
        );
    }
    let (bare, enriched) = fixture.open();
    let source = NodeSelector::Match {
        label: "Person".into(),
        property: "name".into(),
        value: PropValue::Str("Alice".into()),
    };
    let target = NodeSelector::Match {
        label: "Person".into(),
        property: "name".into(),
        value: PropValue::Str("Dave".into()),
    };

    for case in [
        PairedAlgorithmCase::Paths(PathAlgorithm::MinCostMaxFlow),
        PairedAlgorithmCase::Paths(PathAlgorithm::MinCostMaxFlowEdges),
    ] {
        let algorithm = case.algorithm();
        let bare_result = case
            .execute(&bare, &source, &target)
            .unwrap_or_else(|error| panic!("{algorithm:?} failed on bare clone: {error}"));
        let enriched_result = case
            .execute(&enriched, &source, &target)
            .unwrap_or_else(|error| panic!("{algorithm:?} failed on enriched clone: {error}"));

        assert_eq!(
            bare_result.schema(),
            enriched_result.schema(),
            "{algorithm:?} schema"
        );
        assert_eq!(
            bare_result.num_rows(),
            enriched_result.num_rows(),
            "{algorithm:?} rows"
        );
        assert_eq!(
            bare_result.columns(),
            enriched_result.columns(),
            "{algorithm:?} values"
        );
        assert_no_forbidden_knowledge_names(bare_result.schema().as_ref(), algorithm);
    }
}

fn assert_no_forbidden_knowledge_names(schema: &Schema, algorithm: Algorithm) {
    for name in schema
        .fields()
        .iter()
        .flat_map(|field| std::iter::once(field.name()).chain(field.metadata().keys()))
        .chain(schema.metadata().keys())
    {
        let normalized = name.to_ascii_lowercase().replace(['-', '.'], "_");
        assert!(
            !FORBIDDEN_KNOWLEDGE_NAMES.iter().any(|forbidden| {
                normalized == *forbidden
                    || (!matches!(*forbidden, "from" | "to") && normalized.contains(forbidden))
            }),
            "{algorithm:?} exposes forbidden public schema name {name:?}"
        );
    }
}

fn complete_catalog() -> Vec<Algorithm> {
    RankAlgorithm::ALL
        .iter()
        .copied()
        .map(Algorithm::Rank)
        .chain(
            ClusterAlgorithm::ALL
                .iter()
                .copied()
                .map(Algorithm::Cluster),
        )
        .chain(
            SimilarAlgorithm::ALL
                .iter()
                .copied()
                .map(Algorithm::Similar),
        )
        .chain(PathAlgorithm::ALL.iter().copied().map(Algorithm::Paths))
        .chain(
            AnalyzeAlgorithm::ALL
                .iter()
                .copied()
                .map(Algorithm::Analyze),
        )
        .collect()
}

fn catalog_partition() -> HashMap<Algorithm, CatalogDisposition> {
    complete_catalog()
        .into_iter()
        .map(|algorithm| {
            let disposition = match algorithm {
                Algorithm::Rank(_) | Algorithm::Cluster(_) => {
                    if RANK_CLUSTER_CASES
                        .iter()
                        .chain(REMAINING_CASES)
                        .any(|case| case.algorithm() == algorithm)
                    {
                        CatalogDisposition::ExecutablePairedCase
                    } else {
                        CatalogDisposition::ExpectedUnavailable
                    }
                }
                Algorithm::Similar(_) => CatalogDisposition::ExecutablePairedCase,
                Algorithm::Paths(by) => matches!(
                    by,
                    PathAlgorithm::Bfs
                        | PathAlgorithm::Dijkstra
                        | PathAlgorithm::DijkstraAllPairs
                        | PathAlgorithm::AStar
                        | PathAlgorithm::BellmanFord
                        | PathAlgorithm::FloydWarshall
                        | PathAlgorithm::DeltaStepping
                        | PathAlgorithm::Yens
                        | PathAlgorithm::Dfs
                        | PathAlgorithm::TransitiveClosure
                        | PathAlgorithm::MaxFlow
                        | PathAlgorithm::MaxFlowEdges
                        | PathAlgorithm::MinCut
                        | PathAlgorithm::MinCutEdges
                        | PathAlgorithm::MinCostMaxFlow
                        | PathAlgorithm::MinCostMaxFlowEdges
                        | PathAlgorithm::RandomWalk
                        | PathAlgorithm::GomoryHuTree
                        | PathAlgorithm::MinSteinerTree
                        | PathAlgorithm::PrizeCollectingSteinerTree
                )
                .then_some(CatalogDisposition::ExecutablePairedCase)
                .unwrap_or(CatalogDisposition::ExpectedUnavailable),
                Algorithm::Analyze(by) => matches!(
                    by,
                    AnalyzeAlgorithm::ArticulationPoints
                        | AnalyzeAlgorithm::Bridges
                        | AnalyzeAlgorithm::ChromaticNumber
                        | AnalyzeAlgorithm::CountAutomorphisms
                        | AnalyzeAlgorithm::MinimumSpanningTree
                        | AnalyzeAlgorithm::MinimumKSpanningTree
                        | AnalyzeAlgorithm::MaximumSpanningTree
                        | AnalyzeAlgorithm::TriangleCount
                        | AnalyzeAlgorithm::TriadCensus
                        | AnalyzeAlgorithm::Transitivity
                        | AnalyzeAlgorithm::IsPlanar
                        | AnalyzeAlgorithm::NodeColoring
                        | AnalyzeAlgorithm::K1Coloring
                        | AnalyzeAlgorithm::FindCycles
                        | AnalyzeAlgorithm::DagLongestPath
                        | AnalyzeAlgorithm::DagLongestPathWeighted
                        | AnalyzeAlgorithm::EdgeColoring
                        | AnalyzeAlgorithm::EulerCircuit
                        | AnalyzeAlgorithm::EulerPath
                        | AnalyzeAlgorithm::HasEulerCircuit
                        | AnalyzeAlgorithm::HasEulerPath
                        | AnalyzeAlgorithm::TopologicalSort
                        | AnalyzeAlgorithm::IsDag
                        | AnalyzeAlgorithm::DyadCensus
                        | AnalyzeAlgorithm::Conductance
                        | AnalyzeAlgorithm::Modularity
                        | AnalyzeAlgorithm::MaxBipartiteMatching
                        | AnalyzeAlgorithm::MaxCardinalityMatching
                        | AnalyzeAlgorithm::MaxWeightMatching
                )
                .then_some(CatalogDisposition::ExecutablePairedCase)
                .unwrap_or_else(|| {
                    EMBEDDING_CASES
                        .iter()
                        .find(|case| case.by == by)
                        .map(|case| case.disposition)
                        .unwrap_or(CatalogDisposition::ExpectedUnavailable)
                }),
            };
            (algorithm, disposition)
        })
        .collect()
}

fn assert_algorithm_parity(
    algorithm: Algorithm,
    left: Result<RecordBatch, graphforge_core::GfError>,
    right: Result<RecordBatch, graphforge_core::GfError>,
    expect_unavailable: bool,
) {
    match (left, right) {
        (Ok(left), Ok(right)) => {
            assert!(
                !expect_unavailable,
                "{algorithm:?} was marked unavailable but now has a production handler"
            );
            assert_eq!(left.schema(), right.schema(), "{algorithm:?} schema");
            assert_eq!(left.num_rows(), right.num_rows(), "{algorithm:?} rows");
            assert_eq!(left.columns(), right.columns(), "{algorithm:?} values");
            assert_no_forbidden_knowledge_names(left.schema().as_ref(), algorithm);
        }
        (Err(left), Err(right)) => {
            assert!(
                expect_unavailable,
                "{algorithm:?} failed on both isolation clones: {left}"
            );
            let expected = format!(
                "Rust algorithm capability is unavailable: {}.{}",
                algorithm.verb().as_str(),
                algorithm.as_str()
            );
            assert_eq!(left.to_string(), right.to_string(), "{algorithm:?}");
            assert!(
                left.to_string().contains(&expected),
                "{algorithm:?} was marked unavailable but returned {left}"
            );
        }
        (left, right) => panic!(
            "{algorithm:?} was marked unavailable but did not fail symmetrically: \
             bare={left:?}, enriched={right:?}"
        ),
    }
}

fn assert_query_parity(
    query: &str,
    left: Result<ExecutionResult, graphforge_core::GfError>,
    right: Result<ExecutionResult, graphforge_core::GfError>,
) {
    match (left, right) {
        (Ok(left), Ok(right)) => {
            assert_eq!(
                normalize_query_schema(left.schema.as_ref()),
                normalize_query_schema(right.schema.as_ref()),
                "{query}"
            );
            assert_eq!(left.batches.len(), right.batches.len(), "{query}");
            for (index, (left, right)) in left.batches.iter().zip(right.batches.iter()).enumerate()
            {
                assert_eq!(left.num_rows(), right.num_rows(), "{query}, batch {index}");
                assert_eq!(left.columns(), right.columns(), "{query}, batch {index}");
            }
        }
        (Err(left), Err(right)) => {
            assert_eq!(left.to_string(), right.to_string(), "{query}");
            panic!("{query} failed on both clones: {left}");
        }
        (left, right) => {
            panic!("{query} failed asymmetrically: bare={left:?}, enriched={right:?}")
        }
    }
}

fn normalize_query_schema(schema: &Schema) -> Schema {
    let metadata = schema
        .metadata()
        .iter()
        .filter(|(key, _)| !EPHEMERAL_QUERY_METADATA.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Schema::new_with_metadata(schema.fields().clone(), metadata)
}

fn inject_synthetic_knowledge(project: &Path) {
    for subtree in [
        "assertions",
        "evidence",
        "reasoning",
        "supersession",
        "valid_time",
    ] {
        let dir = project.join("knowledge").join(subtree);
        fs::create_dir_all(&dir).expect("create synthetic knowledge subtree");
        fs::write(
            dir.join("SYNTHETIC_TEST_ONLY.json"),
            format!(
                "{{\"synthetic_test_only\":true,\"noncanonical\":true,\"kind\":\"{subtree}\",\"confidence\":0.999,\"belief\":\"accepted\",\"prize\":1000000}}\n"
            ),
        )
        .expect("write synthetic knowledge sentinel");
    }
    let provenance = project.join("provenance");
    fs::create_dir_all(&provenance).expect("create synthetic provenance subtree");
    fs::write(
        provenance.join("SYNTHETIC_TEST_ONLY.json"),
        "{\"synthetic_test_only\":true,\"noncanonical\":true,\"kind\":\"provenance\"}\n",
    )
    .expect("write synthetic provenance sentinel");
}

fn assert_graph_native_state_equal(left: &Path, right: &Path) {
    assert_eq!(
        snapshot_tree_excluding_reserved(left),
        snapshot_tree_excluding_reserved(right),
        "topology, properties, runtime catalog, and graph-native state diverged"
    );
}

fn snapshot_tree_excluding_reserved(root: &Path) -> HashMap<PathBuf, Vec<u8>> {
    snapshot_tree(root)
        .into_iter()
        .filter(|(path, _)| {
            !matches!(
                path.components()
                    .next()
                    .and_then(|part| part.as_os_str().to_str()),
                Some("provenance" | "knowledge")
            )
        })
        .collect()
}

fn snapshot_reserved_sidecars(root: &Path) -> HashMap<PathBuf, Vec<u8>> {
    snapshot_tree(root)
        .into_iter()
        .filter(|(path, _)| {
            matches!(
                path.components()
                    .next()
                    .and_then(|part| part.as_os_str().to_str()),
                Some("provenance" | "knowledge")
            )
        })
        .collect()
}

fn snapshot_tree(root: &Path) -> HashMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, files: &mut HashMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(dir).expect("read fixture directory") {
            let entry = entry.expect("fixture entry");
            let path = entry.path();
            if entry.file_type().expect("fixture file type").is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .expect("relative fixture path")
                        .into(),
                    fs::read(path).expect("read fixture file"),
                );
            }
        }
    }
    let mut files = HashMap::new();
    visit(root, root, &mut files);
    files
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create clone directory");
    for entry in fs::read_dir(source).expect("read seed directory") {
        let entry = entry.expect("seed entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type().expect("seed file type").is_dir() {
            copy_tree(&source_path, &destination_path);
        } else {
            fs::copy(source_path, destination_path).expect("copy seed file");
        }
    }
}

fn remove_if_present(path: &Path) {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("remove {}: {error}", path.display()),
    }
}
