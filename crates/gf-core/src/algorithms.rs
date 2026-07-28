//! Typed registry and stable logical result schemas for analyst algorithms.
//!
//! Algorithm execution belongs to `gf-exec`; this module is intentionally
//! dependency-light so the Rust API and every binding share one vocabulary.

use std::{fmt, str::FromStr};

use crate::GfError;

/// Analyst-intent API surface that owns an algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlgorithmVerb {
    /// Node scoring.
    Rank,
    /// Community assignment and decomposition.
    Cluster,
    /// Pairwise similarity.
    Similar,
    /// Paths, traversal, reachability, and flow.
    Paths,
    /// Graph-level and structural analysis.
    Analyze,
}

impl AlgorithmVerb {
    /// Public method name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rank => "rank",
            Self::Cluster => "cluster",
            Self::Similar => "similar",
            Self::Paths => "paths",
            Self::Analyze => "analyze",
        }
    }
}

macro_rules! algorithm_enum {
    (
        $name:ident, $default:ident, $verb:expr,
        { $($variant:ident => $wire:literal),+ $(,)? }
        $(, aliases { $($alias:literal => $alias_variant:ident),+ $(,)? })?
    ) => {
        #[doc = concat!("Typed `by=` values for `", stringify!($name), "`.")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $(
                #[doc = concat!("`", $wire, "`.")]
                $variant,
            )+
        }

        impl $name {
            /// Every canonical value in deterministic catalog order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Canonical public `by=` value.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $wire),+ }
            }

            /// Owning analyst verb.
            #[must_use]
            pub const fn verb(self) -> AlgorithmVerb { $verb }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = GfError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($wire => Ok(Self::$variant),)+
                    $($($alias => Ok(Self::$alias_variant),)+)?
                    _ => Err(unknown_algorithm($verb, value)),
                }
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::$default }
        }
    };
}

algorithm_enum!(RankAlgorithm, PageRank, AlgorithmVerb::Rank, {
    PageRank => "pagerank",
    Betweenness => "betweenness",
    Closeness => "closeness",
    HarmonicCloseness => "harmonic_closeness",
    Degree => "degree",
    Eigenvector => "eigenvector",
    ArticleRank => "article_rank",
    HitsHub => "hits_hub",
    HitsAuthority => "hits_authority",
    Celf => "celf",
    ClusteringCoefficient => "clustering_coefficient",
    Triangles => "triangles",
    KCore => "k_core",
    PreferentialAttachment => "preferential_attachment",
    AdamicAdar => "adamic_adar",
    CommonNeighbors => "common_neighbors",
    ResourceAllocation => "resource_allocation",
    TotalNeighbors => "total_neighbors",
}, aliases {
    "local_clustering_coefficient" => ClusteringCoefficient,
});

algorithm_enum!(ClusterAlgorithm, Louvain, AlgorithmVerb::Cluster, {
    Louvain => "louvain",
    Leiden => "leiden",
    LabelPropagation => "label_propagation",
    SpeakerListener => "speaker_listener",
    GirvanNewman => "girvan_newman",
    ModularityOptimization => "modularity_optimization",
    FastGreedy => "fastgreedy",
    InfoMap => "infomap",
    LeadingEigenvector => "leading_eigenvector",
    Walktrap => "walktrap",
    Spinglass => "spinglass",
    Hdbscan => "hdbscan",
    KMeans => "k_means",
    ApproximateMaxKCut => "approximate_max_k_cut",
    Components => "components",
    StronglyConnected => "strongly_connected",
    Biconnected => "biconnected",
    KCoreDecomposition => "k_core_decomposition",
});

impl ClusterAlgorithm {
    /// Whether this algorithm consumes edge orientation.
    #[must_use]
    pub const fn respects_direction(self) -> bool {
        match self {
            Self::InfoMap
            | Self::LeadingEigenvector
            | Self::Walktrap
            | Self::Components
            | Self::StronglyConnected => true,
            Self::Louvain
            | Self::Leiden
            | Self::LabelPropagation
            | Self::SpeakerListener
            | Self::GirvanNewman
            | Self::ModularityOptimization
            | Self::FastGreedy
            | Self::Spinglass
            | Self::Hdbscan
            | Self::KMeans
            | Self::ApproximateMaxKCut
            | Self::Biconnected
            | Self::KCoreDecomposition => false,
        }
    }
}

algorithm_enum!(SimilarAlgorithm, NodeSimilarity, AlgorithmVerb::Similar, {
    NodeSimilarity => "node_similarity",
    Knn => "knn",
    FilteredKnn => "filtered_knn",
    FilteredNodeSimilarity => "filtered_node_similarity",
    Cosine => "cosine",
});

algorithm_enum!(PathAlgorithm, Bfs, AlgorithmVerb::Paths, {
    Bfs => "bfs",
    Dijkstra => "dijkstra",
    DijkstraAllPairs => "dijkstra_all_pairs",
    AStar => "astar",
    BellmanFord => "bellman_ford",
    FloydWarshall => "floyd_warshall",
    DeltaStepping => "delta_stepping",
    Yens => "yens",
    MaxFlow => "max_flow",
    MaxFlowEdges => "max_flow_edges",
    MinCut => "min_cut",
    MinCutEdges => "min_cut_edges",
    MinCostMaxFlow => "min_cost_max_flow",
    MinCostMaxFlowEdges => "min_cost_max_flow_edges",
    GomoryHuTree => "gomory_hu_tree",
    MinSteinerTree => "min_steiner_tree",
    PrizeCollectingSteinerTree => "prize_collecting_steiner_tree",
    Dfs => "dfs",
    RandomWalk => "random_walk",
    TransitiveClosure => "transitive_closure",
});

algorithm_enum!(AnalyzeAlgorithm, IsDag, AlgorithmVerb::Analyze, {
    MinimumSpanningTree => "minimum_spanning_tree",
    MaximumSpanningTree => "maximum_spanning_tree",
    MinimumKSpanningTree => "minimum_k_spanning_tree",
    TopologicalSort => "topological_sort",
    IsDag => "is_dag",
    FindCycles => "find_cycles",
    DagLongestPath => "dag_longest_path",
    DagLongestPathWeighted => "dag_longest_path_weighted",
    NodeColoring => "node_coloring",
    EdgeColoring => "edge_coloring",
    ChromaticNumber => "chromatic_number",
    K1Coloring => "k1_coloring",
    MaxWeightMatching => "max_weight_matching",
    MaxCardinalityMatching => "max_cardinality_matching",
    MaxBipartiteMatching => "max_bipartite_matching",
    EulerCircuit => "euler_circuit",
    EulerPath => "euler_path",
    HasEulerCircuit => "has_euler_circuit",
    HasEulerPath => "has_euler_path",
    IsPlanar => "is_planar",
    ArticulationPoints => "articulation_points",
    Bridges => "bridges",
    TriangleCount => "triangle_count",
    Conductance => "conductance",
    Modularity => "modularity",
    Transitivity => "transitivity",
    TriadCensus => "triad_census",
    DyadCensus => "dyad_census",
    CountAutomorphisms => "count_automorphisms",
    Node2Vec => "node2vec",
    GraphSage => "graphsage",
    FastRandomProjection => "fast_random_projection",
    HashGnn => "hashgnn",
});

/// A typed algorithm from any analyst verb.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Algorithm {
    /// Rank algorithm.
    Rank(RankAlgorithm),
    /// Cluster algorithm.
    Cluster(ClusterAlgorithm),
    /// Similarity algorithm.
    Similar(SimilarAlgorithm),
    /// Path algorithm.
    Paths(PathAlgorithm),
    /// Analysis algorithm.
    Analyze(AnalyzeAlgorithm),
}

/// Language-independent type in an algorithm result schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlgorithmFieldType {
    /// Arrow `FixedSizeBinary(16)` public UUID identity.
    Uuid,
    /// Arrow `List<FixedSizeBinary(16)>` UUID sequence.
    UuidList,
    /// Arrow `List<Float32>` embedding.
    Float32List,
    /// Arrow `Utf8`.
    Utf8,
    /// Arrow `Boolean`.
    Boolean,
    /// Arrow `UInt64`.
    UInt64,
    /// Arrow `Int64`.
    Int64,
    /// Arrow `Float64`.
    Float64,
}

/// Required field in a stable logical result schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AlgorithmField {
    /// Stable public column name.
    pub name: &'static str,
    /// Logical Arrow type.
    pub data_type: AlgorithmFieldType,
    /// Whether the field accepts nulls.
    pub nullable: bool,
}

const fn field(
    name: &'static str,
    data_type: AlgorithmFieldType,
    nullable: bool,
) -> AlgorithmField {
    AlgorithmField {
        name,
        data_type,
        nullable,
    }
}

/// Stable logical schema required from an algorithm implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlgorithmResultSchema {
    /// Required fields in public output order.
    pub fields: &'static [AlgorithmField],
    /// Node properties follow the required fields for node-oriented verbs.
    pub includes_node_properties: bool,
}

const fn schema(fields: &'static [AlgorithmField]) -> AlgorithmResultSchema {
    AlgorithmResultSchema {
        fields,
        includes_node_properties: false,
    }
}

const fn node_schema(fields: &'static [AlgorithmField]) -> AlgorithmResultSchema {
    AlgorithmResultSchema {
        fields,
        includes_node_properties: true,
    }
}

const NODE_SCORE: &[AlgorithmField] = &[
    field("node_uuid", AlgorithmFieldType::Uuid, false),
    field("score", AlgorithmFieldType::Float64, false),
];
const NODE_COMMUNITY: &[AlgorithmField] = &[
    field("node_uuid", AlgorithmFieldType::Uuid, false),
    field("community_id", AlgorithmFieldType::Int64, false),
];
const SIMILARITY: &[AlgorithmField] = &[
    field("node1_uuid", AlgorithmFieldType::Uuid, false),
    field("node2_uuid", AlgorithmFieldType::Uuid, false),
    field("similarity", AlgorithmFieldType::Float64, false),
];
const PATH: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("cost", AlgorithmFieldType::Float64, false),
    field("path", AlgorithmFieldType::UuidList, false),
];
const RANKED_PATH: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("rank", AlgorithmFieldType::UInt64, false),
    field("cost", AlgorithmFieldType::Float64, false),
    field("path", AlgorithmFieldType::UuidList, false),
];
const FLOW: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("sink_uuid", AlgorithmFieldType::Uuid, false),
    field("flow", AlgorithmFieldType::Float64, false),
];
const FLOW_EDGES: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("flow", AlgorithmFieldType::Float64, false),
];
const COSTED_FLOW: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("sink_uuid", AlgorithmFieldType::Uuid, false),
    field("flow", AlgorithmFieldType::Float64, false),
    field("cost", AlgorithmFieldType::Float64, false),
];
const COSTED_FLOW_EDGES: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("flow", AlgorithmFieldType::Float64, false),
    field("unit_cost", AlgorithmFieldType::Float64, false),
    field("flow_cost", AlgorithmFieldType::Float64, false),
];
const MIN_CUT: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("sink_uuid", AlgorithmFieldType::Uuid, false),
    field("cut_value", AlgorithmFieldType::Float64, false),
];
const MIN_CUT_EDGES: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("capacity", AlgorithmFieldType::Float64, false),
];
const CUT: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("cut_value", AlgorithmFieldType::Float64, false),
];
const EDGE_LIST: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("weight", AlgorithmFieldType::Float64, true),
];
const STEINER_EDGE_LIST: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("weight", AlgorithmFieldType::Float64, false),
];
const UNWEIGHTED_EDGE_LIST: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
];
const K_EDGE_LIST: &[AlgorithmField] = &[
    field("tree_id", AlgorithmFieldType::UInt64, false),
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
    field("weight", AlgorithmFieldType::Float64, false),
];
const TRAVERSAL: &[AlgorithmField] = &[
    field("node_uuid", AlgorithmFieldType::Uuid, false),
    field("depth", AlgorithmFieldType::UInt64, false),
    field("order", AlgorithmFieldType::UInt64, false),
];
const WALK: &[AlgorithmField] = &[
    field("start_uuid", AlgorithmFieldType::Uuid, false),
    field("walk", AlgorithmFieldType::UuidList, false),
];
const PAIR: &[AlgorithmField] = &[
    field("source_uuid", AlgorithmFieldType::Uuid, false),
    field("target_uuid", AlgorithmFieldType::Uuid, false),
];
const NODE_ORDER: &[AlgorithmField] = &[
    field("node_uuid", AlgorithmFieldType::Uuid, false),
    field("order", AlgorithmFieldType::UInt64, false),
];
const NODE: &[AlgorithmField] = &[field("node_uuid", AlgorithmFieldType::Uuid, false)];
const NODE_COLOR: &[AlgorithmField] = &[
    field("node_uuid", AlgorithmFieldType::Uuid, false),
    field("color", AlgorithmFieldType::UInt64, false),
];
const EDGE_COLOR: &[AlgorithmField] = &[
    field("edge_uuid", AlgorithmFieldType::Uuid, false),
    field("color", AlgorithmFieldType::UInt64, false),
];
const EULER_TRAIL: &[AlgorithmField] = &[
    field("node_path", AlgorithmFieldType::UuidList, false),
    field("edge_path", AlgorithmFieldType::UuidList, false),
];
const CYCLE: &[AlgorithmField] = &[field("cycle", AlgorithmFieldType::UuidList, false)];
const COST_PATH: &[AlgorithmField] = &[
    field("cost", AlgorithmFieldType::Float64, false),
    field("path", AlgorithmFieldType::UuidList, false),
];
const IS_DAG: &[AlgorithmField] = &[field("is_dag", AlgorithmFieldType::Boolean, false)];
const HAS_EULER_CIRCUIT: &[AlgorithmField] = &[field(
    "has_euler_circuit",
    AlgorithmFieldType::Boolean,
    false,
)];
const HAS_EULER_PATH: &[AlgorithmField] =
    &[field("has_euler_path", AlgorithmFieldType::Boolean, false)];
const IS_PLANAR: &[AlgorithmField] = &[field("is_planar", AlgorithmFieldType::Boolean, false)];
const CHROMATIC_NUMBER: &[AlgorithmField] =
    &[field("chromatic_number", AlgorithmFieldType::UInt64, false)];
const TRIANGLE_COUNT: &[AlgorithmField] =
    &[field("triangle_count", AlgorithmFieldType::UInt64, false)];
const AUTOMORPHISM_COUNT: &[AlgorithmField] = &[field("count", AlgorithmFieldType::UInt64, false)];
const CONDUCTANCE: &[AlgorithmField] = &[
    field("partition_id", AlgorithmFieldType::Utf8, false),
    field("conductance", AlgorithmFieldType::Float64, false),
];
const MODULARITY: &[AlgorithmField] = &[field("modularity", AlgorithmFieldType::Float64, false)];
const TRANSITIVITY: &[AlgorithmField] =
    &[field("transitivity", AlgorithmFieldType::Float64, false)];
const TRIAD_CENSUS: &[AlgorithmField] = &[
    field("triad_type", AlgorithmFieldType::Utf8, false),
    field("count", AlgorithmFieldType::UInt64, false),
];
const DYAD_CENSUS: &[AlgorithmField] = &[
    field("dyad_type", AlgorithmFieldType::Utf8, false),
    field("count", AlgorithmFieldType::UInt64, false),
];
const EMBEDDING: &[AlgorithmField] = &[
    field("node_uuid", AlgorithmFieldType::Uuid, false),
    field("embedding", AlgorithmFieldType::Float32List, false),
];

impl Algorithm {
    /// Parse a public `by=` value in the namespace of its verb.
    pub fn parse(verb: AlgorithmVerb, value: &str) -> Result<Self, GfError> {
        match verb {
            AlgorithmVerb::Rank => value.parse().map(Self::Rank),
            AlgorithmVerb::Cluster => value.parse().map(Self::Cluster),
            AlgorithmVerb::Similar => value.parse().map(Self::Similar),
            AlgorithmVerb::Paths => value.parse().map(Self::Paths),
            AlgorithmVerb::Analyze => value.parse().map(Self::Analyze),
        }
    }

    /// Owning public verb.
    #[must_use]
    pub const fn verb(self) -> AlgorithmVerb {
        match self {
            Self::Rank(_) => AlgorithmVerb::Rank,
            Self::Cluster(_) => AlgorithmVerb::Cluster,
            Self::Similar(_) => AlgorithmVerb::Similar,
            Self::Paths(_) => AlgorithmVerb::Paths,
            Self::Analyze(_) => AlgorithmVerb::Analyze,
        }
    }

    /// Canonical public `by=` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rank(value) => value.as_str(),
            Self::Cluster(value) => value.as_str(),
            Self::Similar(value) => value.as_str(),
            Self::Paths(value) => value.as_str(),
            Self::Analyze(value) => value.as_str(),
        }
    }

    /// Stable logical result schema required from every language surface.
    #[must_use]
    pub const fn result_schema(self) -> AlgorithmResultSchema {
        match self {
            Self::Rank(_) => node_schema(NODE_SCORE),
            Self::Cluster(_) => node_schema(NODE_COMMUNITY),
            Self::Similar(_) => schema(SIMILARITY),
            Self::Paths(value) => path_schema(value),
            Self::Analyze(value) => analyze_schema(value),
        }
    }
}

const fn path_schema(value: PathAlgorithm) -> AlgorithmResultSchema {
    match value {
        PathAlgorithm::Yens => schema(RANKED_PATH),
        PathAlgorithm::MaxFlow => schema(FLOW),
        PathAlgorithm::MaxFlowEdges => schema(FLOW_EDGES),
        PathAlgorithm::MinCut => schema(MIN_CUT),
        PathAlgorithm::MinCutEdges => schema(MIN_CUT_EDGES),
        PathAlgorithm::MinCostMaxFlow => schema(COSTED_FLOW),
        PathAlgorithm::MinCostMaxFlowEdges => schema(COSTED_FLOW_EDGES),
        PathAlgorithm::GomoryHuTree => schema(CUT),
        PathAlgorithm::MinSteinerTree | PathAlgorithm::PrizeCollectingSteinerTree => {
            schema(STEINER_EDGE_LIST)
        }
        PathAlgorithm::Dfs => schema(TRAVERSAL),
        PathAlgorithm::RandomWalk => schema(WALK),
        PathAlgorithm::TransitiveClosure => schema(PAIR),
        _ => schema(PATH),
    }
}

const fn analyze_schema(value: AnalyzeAlgorithm) -> AlgorithmResultSchema {
    match value {
        AnalyzeAlgorithm::MinimumKSpanningTree => schema(K_EDGE_LIST),
        AnalyzeAlgorithm::MinimumSpanningTree
        | AnalyzeAlgorithm::MaximumSpanningTree
        | AnalyzeAlgorithm::MaxWeightMatching => schema(EDGE_LIST),
        AnalyzeAlgorithm::MaxCardinalityMatching
        | AnalyzeAlgorithm::MaxBipartiteMatching
        | AnalyzeAlgorithm::Bridges => schema(UNWEIGHTED_EDGE_LIST),
        AnalyzeAlgorithm::TopologicalSort => schema(NODE_ORDER),
        AnalyzeAlgorithm::ArticulationPoints => schema(NODE),
        AnalyzeAlgorithm::IsDag => schema(IS_DAG),
        AnalyzeAlgorithm::FindCycles => schema(CYCLE),
        AnalyzeAlgorithm::DagLongestPath | AnalyzeAlgorithm::DagLongestPathWeighted => {
            schema(COST_PATH)
        }
        AnalyzeAlgorithm::NodeColoring | AnalyzeAlgorithm::K1Coloring => schema(NODE_COLOR),
        AnalyzeAlgorithm::EdgeColoring => schema(EDGE_COLOR),
        AnalyzeAlgorithm::ChromaticNumber => schema(CHROMATIC_NUMBER),
        AnalyzeAlgorithm::EulerCircuit | AnalyzeAlgorithm::EulerPath => schema(EULER_TRAIL),
        AnalyzeAlgorithm::HasEulerCircuit => schema(HAS_EULER_CIRCUIT),
        AnalyzeAlgorithm::HasEulerPath => schema(HAS_EULER_PATH),
        AnalyzeAlgorithm::IsPlanar => schema(IS_PLANAR),
        AnalyzeAlgorithm::TriangleCount => schema(TRIANGLE_COUNT),
        AnalyzeAlgorithm::Conductance => schema(CONDUCTANCE),
        AnalyzeAlgorithm::Modularity => schema(MODULARITY),
        AnalyzeAlgorithm::Transitivity => schema(TRANSITIVITY),
        AnalyzeAlgorithm::TriadCensus => schema(TRIAD_CENSUS),
        AnalyzeAlgorithm::DyadCensus => schema(DYAD_CENSUS),
        AnalyzeAlgorithm::CountAutomorphisms => schema(AUTOMORPHISM_COUNT),
        AnalyzeAlgorithm::Node2Vec
        | AnalyzeAlgorithm::GraphSage
        | AnalyzeAlgorithm::FastRandomProjection
        | AnalyzeAlgorithm::HashGnn => schema(EMBEDDING),
    }
}

fn unknown_algorithm(verb: AlgorithmVerb, value: &str) -> GfError {
    GfError::Validation(format!(
        "unknown {} algorithm `{value}`; use a catalogued `by=` value",
        verb.as_str()
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn every_catalog_is_unique_and_round_trips() {
        let rank = RankAlgorithm::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>();
        let cluster = ClusterAlgorithm::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>();
        let similar = SimilarAlgorithm::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>();
        let paths = PathAlgorithm::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>();
        let analyze = AnalyzeAlgorithm::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<Vec<_>>();
        let catalogs: &[(&[&str], AlgorithmVerb)] = &[
            (&rank, AlgorithmVerb::Rank),
            (&cluster, AlgorithmVerb::Cluster),
            (&similar, AlgorithmVerb::Similar),
            (&paths, AlgorithmVerb::Paths),
            (&analyze, AlgorithmVerb::Analyze),
        ];

        for (names, verb) in catalogs {
            let unique: HashSet<_> = names.iter().copied().collect();
            assert_eq!(unique.len(), names.len(), "duplicate in {}", verb.as_str());
            for name in *names {
                let parsed = Algorithm::parse(*verb, name).expect("catalog entry must parse");
                assert_eq!(parsed.as_str(), *name);
                assert_eq!(parsed.verb(), *verb);
                assert!(!parsed.result_schema().fields.is_empty());
            }
        }
    }

    #[test]
    fn rank_alias_has_one_canonical_owner() {
        let alias: RankAlgorithm = "local_clustering_coefficient".parse().unwrap();
        assert_eq!(alias, RankAlgorithm::ClusteringCoefficient);
        assert_eq!(alias.as_str(), "clustering_coefficient");
        assert!(
            !RankAlgorithm::ALL
                .iter()
                .any(|value| value.as_str() == "local_clustering_coefficient")
        );
    }

    #[test]
    fn invalid_name_is_a_stable_validation_error() {
        let error = Algorithm::parse(AlgorithmVerb::Similar, "pagerank").unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert_eq!(
            error.to_string(),
            "validation error: unknown similar algorithm `pagerank`; use a catalogued `by=` value"
        );
    }

    #[test]
    fn vertical_proof_schemas_are_stable_and_uuid_only() {
        let cases = [
            (
                Algorithm::Rank(RankAlgorithm::Degree),
                &["node_uuid", "score"][..],
            ),
            (
                Algorithm::Cluster(ClusterAlgorithm::Components),
                &["node_uuid", "community_id"],
            ),
            (
                Algorithm::Similar(SimilarAlgorithm::NodeSimilarity),
                &["node1_uuid", "node2_uuid", "similarity"],
            ),
            (
                Algorithm::Paths(PathAlgorithm::Bfs),
                &["source_uuid", "target_uuid", "cost", "path"],
            ),
            (Algorithm::Analyze(AnalyzeAlgorithm::IsDag), &["is_dag"]),
        ];
        for (algorithm, names) in cases {
            let fields = algorithm.result_schema().fields;
            assert_eq!(
                fields.iter().map(|field| field.name).collect::<Vec<_>>(),
                names
            );
            assert!(fields.iter().all(|field| {
                ![
                    "node_id",
                    "edge_id",
                    "src_id",
                    "dst_id",
                    "source_id",
                    "target_id",
                ]
                .contains(&field.name)
            }));
        }
    }

    #[test]
    fn max_cardinality_matching_has_an_unweighted_edge_schema() {
        let fields = Algorithm::Analyze(AnalyzeAlgorithm::MaxCardinalityMatching)
            .result_schema()
            .fields;

        assert_eq!(
            fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", AlgorithmFieldType::Uuid, false),
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("target_uuid", AlgorithmFieldType::Uuid, false),
            ]
        );
    }

    #[test]
    fn max_bipartite_matching_has_an_unweighted_edge_schema() {
        let fields = Algorithm::Analyze(AnalyzeAlgorithm::MaxBipartiteMatching)
            .result_schema()
            .fields;

        assert_eq!(
            fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", AlgorithmFieldType::Uuid, false),
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("target_uuid", AlgorithmFieldType::Uuid, false),
            ]
        );
    }

    #[test]
    fn max_flow_catalog_values_have_separate_stable_schemas() {
        let scalar: PathAlgorithm = "max_flow".parse().unwrap();
        let edges: PathAlgorithm = "max_flow_edges".parse().unwrap();

        assert_eq!(scalar, PathAlgorithm::MaxFlow);
        assert_eq!(scalar.as_str(), "max_flow");
        assert_eq!(edges, PathAlgorithm::MaxFlowEdges);
        assert_eq!(edges.as_str(), "max_flow_edges");
        assert_eq!(
            Algorithm::Paths(scalar)
                .result_schema()
                .fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("sink_uuid", AlgorithmFieldType::Uuid, false),
                ("flow", AlgorithmFieldType::Float64, false),
            ]
        );
        assert_eq!(
            Algorithm::Paths(edges)
                .result_schema()
                .fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", AlgorithmFieldType::Uuid, false),
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("target_uuid", AlgorithmFieldType::Uuid, false),
                ("flow", AlgorithmFieldType::Float64, false),
            ]
        );
    }

    #[test]
    fn min_cost_flow_catalog_values_have_separate_stable_schemas() {
        let scalar: PathAlgorithm = "min_cost_max_flow".parse().unwrap();
        let edges: PathAlgorithm = "min_cost_max_flow_edges".parse().unwrap();
        assert_eq!(scalar, PathAlgorithm::MinCostMaxFlow);
        assert_eq!(edges, PathAlgorithm::MinCostMaxFlowEdges);
        assert_eq!(
            Algorithm::Paths(scalar)
                .result_schema()
                .fields
                .iter()
                .map(|field| field.name)
                .collect::<Vec<_>>(),
            ["source_uuid", "sink_uuid", "flow", "cost"]
        );
        assert_eq!(
            Algorithm::Paths(edges)
                .result_schema()
                .fields
                .iter()
                .map(|field| field.name)
                .collect::<Vec<_>>(),
            [
                "edge_uuid",
                "source_uuid",
                "target_uuid",
                "flow",
                "unit_cost",
                "flow_cost",
            ]
        );
        assert!(
            Algorithm::Paths(edges)
                .result_schema()
                .fields
                .iter()
                .all(|field| !field.nullable)
        );
    }

    #[test]
    fn min_cut_catalog_values_have_distinct_stable_uuid_schemas() {
        let scalar: PathAlgorithm = "min_cut".parse().unwrap();
        let edges: PathAlgorithm = "min_cut_edges".parse().unwrap();

        assert_eq!(scalar, PathAlgorithm::MinCut);
        assert_eq!(scalar.as_str(), "min_cut");
        assert_eq!(scalar.to_string(), "min_cut");
        assert_eq!(edges, PathAlgorithm::MinCutEdges);
        assert_eq!(edges.as_str(), "min_cut_edges");
        assert_eq!(edges.to_string(), "min_cut_edges");

        let scalar_schema = Algorithm::Paths(scalar).result_schema();
        assert_eq!(
            scalar_schema
                .fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("sink_uuid", AlgorithmFieldType::Uuid, false),
                ("cut_value", AlgorithmFieldType::Float64, false),
            ]
        );

        let edge_schema = Algorithm::Paths(edges).result_schema();
        assert_eq!(
            edge_schema
                .fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", AlgorithmFieldType::Uuid, false),
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("target_uuid", AlgorithmFieldType::Uuid, false),
                ("capacity", AlgorithmFieldType::Float64, false),
            ]
        );
        assert_ne!(scalar_schema, edge_schema);
        assert!(scalar_schema.fields.iter().all(|field| {
            !["node_id", "edge_id", "source_id", "sink_id", "target_id"].contains(&field.name)
        }));
        assert!(edge_schema.fields.iter().all(|field| {
            !["node_id", "edge_id", "source_id", "sink_id", "target_id"].contains(&field.name)
        }));
    }

    #[test]
    fn euler_constructions_share_aligned_non_null_identity_schema() {
        for algorithm in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
            assert_eq!(
                Algorithm::Analyze(algorithm)
                    .result_schema()
                    .fields
                    .iter()
                    .map(|field| (field.name, field.data_type, field.nullable))
                    .collect::<Vec<_>>(),
                [
                    ("node_path", AlgorithmFieldType::UuidList, false),
                    ("edge_path", AlgorithmFieldType::UuidList, false),
                ]
            );
        }
    }

    #[test]
    fn steiner_algorithms_share_dedicated_non_null_weighted_edge_schema() {
        let ordinary_edge_list =
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumSpanningTree).result_schema();
        assert_eq!(
            ordinary_edge_list
                .fields
                .iter()
                .map(|field| (field.name, field.data_type, field.nullable))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", AlgorithmFieldType::Uuid, false),
                ("source_uuid", AlgorithmFieldType::Uuid, false),
                ("target_uuid", AlgorithmFieldType::Uuid, false),
                ("weight", AlgorithmFieldType::Float64, true),
            ]
        );
        assert!(!ordinary_edge_list.includes_node_properties);
        for algorithm in [
            PathAlgorithm::MinSteinerTree,
            PathAlgorithm::PrizeCollectingSteinerTree,
        ] {
            let steiner = Algorithm::Paths(algorithm).result_schema();
            assert_eq!(
                steiner
                    .fields
                    .iter()
                    .map(|field| (field.name, field.data_type, field.nullable))
                    .collect::<Vec<_>>(),
                [
                    ("edge_uuid", AlgorithmFieldType::Uuid, false),
                    ("source_uuid", AlgorithmFieldType::Uuid, false),
                    ("target_uuid", AlgorithmFieldType::Uuid, false),
                    ("weight", AlgorithmFieldType::Float64, false),
                ]
            );
            assert!(!steiner.includes_node_properties);
            assert_ne!(steiner, ordinary_edge_list);
        }
    }
}
