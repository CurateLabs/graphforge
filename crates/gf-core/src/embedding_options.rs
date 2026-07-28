//! Closed typed options for the four embedding-v1 analysis algorithms.

use crate::algorithms::AnalyzeAlgorithm;

/// Maximum embedding width shared by Node2Vec, GraphSAGE, and FastRP.
pub const MAX_EMBEDDING_DIMENSIONS: usize = 4_096;
/// Maximum embedding width for HashGNN.
pub const MAX_HASHGNN_DIMENSIONS: usize = 8_192;

/// One embedding-v1 option family.
#[derive(Debug, Clone, PartialEq)]
pub enum EmbeddingOptions {
    /// `analyze(by="node2vec")`.
    Node2Vec(Node2VecOptions),
    /// `analyze(by="graphsage")`.
    GraphSage(GraphSageOptions),
    /// `analyze(by="fast_random_projection")`.
    FastRandomProjection(FastRpOptions),
    /// `analyze(by="hashgnn")`.
    HashGnn(HashGnnOptions),
}

/// Graph-native invocation boundary for an embedding analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingAnalyzeOptions {
    /// Embedding algorithm selected from the closed analysis catalog.
    pub by: AnalyzeAlgorithm,
    /// Optional relationship type filter.
    pub via: Option<String>,
    /// Whether to treat edges as directed.
    pub directed: bool,
    /// Optional edge-weight property.
    pub weight: Option<String>,
    /// Closed options for the selected embedding algorithm.
    pub options: EmbeddingOptions,
}

/// Node2Vec v1 options.
#[derive(Debug, Clone, PartialEq)]
pub struct Node2VecOptions {
    /// Output vector width.
    pub dimensions: usize,
    /// Transitions in each walk.
    pub walk_length: usize,
    /// Walks originating at each selected node.
    pub walks_per_node: usize,
    /// Return parameter `p`.
    pub p: f64,
    /// In/out parameter `q`.
    pub q: f64,
    /// Fixed context radius.
    pub window_size: usize,
    /// Negative samples per positive context.
    pub negative_samples: usize,
    /// Training passes over the fixed corpus.
    pub epochs: usize,
    /// Constant SGNS learning rate.
    pub learning_rate: f64,
    /// Caller seed; omission normalizes to zero.
    pub seed: u64,
}

impl Default for Node2VecOptions {
    fn default() -> Self {
        Self {
            dimensions: 128,
            walk_length: 80,
            walks_per_node: 10,
            p: 1.0,
            q: 1.0,
            window_size: 10,
            negative_samples: 5,
            epochs: 1,
            learning_rate: 0.025,
            seed: 0,
        }
    }
}

/// GraphSAGE v1 aggregator. Version 1 intentionally supports only mean.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum GraphSageAggregator {
    /// Arithmetic mean of the sampled neighborhood.
    #[default]
    Mean,
}

/// Unsupervised GraphSAGE v1 options.
///
/// `feature_properties` must be supplied explicitly: the empty default is not a
/// runnable GraphSAGE configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphSageOptions {
    /// Output vector width.
    pub dimensions: usize,
    /// Width of non-final layers.
    pub hidden_dimensions: usize,
    /// Number of aggregation layers.
    pub layers: usize,
    /// Ordered fanout for each layer.
    pub sample_sizes: Vec<usize>,
    /// Version-pinned neighborhood aggregator.
    pub aggregator: GraphSageAggregator,
    /// Training epochs.
    pub epochs: usize,
    /// Negative samples per positive pair.
    pub negative_samples: usize,
    /// Constant Adam learning rate.
    pub learning_rate: f64,
    /// Ordered graph-native numeric feature properties.
    pub feature_properties: Vec<String>,
    /// Caller seed; omission normalizes to zero.
    pub seed: u64,
}

impl Default for GraphSageOptions {
    fn default() -> Self {
        Self {
            dimensions: 256,
            hidden_dimensions: 256,
            layers: 2,
            sample_sizes: vec![25, 10],
            aggregator: GraphSageAggregator::Mean,
            epochs: 1,
            negative_samples: 20,
            learning_rate: 0.000_002,
            feature_properties: Vec::new(),
            seed: 0,
        }
    }
}

/// Fast random projection v1 options.
#[derive(Debug, Clone, PartialEq)]
pub struct FastRpOptions {
    /// Output vector width.
    pub dimensions: usize,
    /// Ordered coefficients for `H_0..H_t`.
    pub iteration_weights: Vec<f64>,
    /// Degree normalization exponent.
    pub normalization_strength: f64,
    /// Weight of the optional feature projection.
    pub feature_weight: f64,
    /// Ordered graph-native scalar feature properties.
    pub feature_properties: Vec<String>,
    /// Caller seed; omission normalizes to zero.
    pub seed: u64,
}

impl Default for FastRpOptions {
    fn default() -> Self {
        Self {
            dimensions: 128,
            iteration_weights: vec![0.0, 1.0, 1.0],
            normalization_strength: 0.0,
            feature_weight: 0.0,
            feature_properties: Vec::new(),
            seed: 0,
        }
    }
}

/// HashGNN v1 options.
#[derive(Debug, Clone, PartialEq)]
pub struct HashGnnOptions {
    /// Output vector width.
    pub dimensions: usize,
    /// Minhash propagation rounds.
    pub iterations: usize,
    /// Initial active-coordinate fraction.
    pub embedding_density: f64,
    /// Whether explicit heterogeneous type properties enter hashing.
    pub heterogeneous: bool,
    /// Explicit graph-native node type property.
    pub node_type_property: Option<String>,
    /// Explicit graph-native relationship type property.
    pub relationship_type_property: Option<String>,
    /// Caller seed; omission normalizes to zero.
    pub seed: u64,
}

impl Default for HashGnnOptions {
    fn default() -> Self {
        Self {
            dimensions: 256,
            iterations: 2,
            embedding_density: 0.25,
            heterogeneous: false,
            node_type_property: None,
            relationship_type_property: None,
            seed: 0,
        }
    }
}
