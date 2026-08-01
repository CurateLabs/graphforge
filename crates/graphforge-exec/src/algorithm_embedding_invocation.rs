//! Neutral deterministic invocation records for persisted embedding runs.

use arrow::record_batch::RecordBatch;
use graphforge_core::embedding_options::{EmbeddingOptions, GraphSageAggregator};

/// Normalized graph selectors consumed before Node2Vec dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingProjectionSelector {
    /// Optional public node-label selector.
    pub label: Option<String>,
    /// Optional public relationship-type selector.
    pub via: Option<String>,
    /// Whether the projection preserves edge orientation.
    pub directed: bool,
    /// Optional graph-native edge-weight property.
    pub weight: Option<String>,
}

/// Actual configured limits governing one embedding execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingInvocationLimits {
    /// Maximum selected nodes.
    pub nodes: u64,
    /// Maximum direction-expanded adjacency entries.
    pub adjacency_entries: u64,
    /// Maximum published rows.
    pub output_rows: u64,
    /// Maximum cooperative algorithm checkpoints.
    pub iterations: u64,
    /// Maximum retained or generated exact-solver states.
    pub states: u64,
    /// Maximum aggregate embedding memory.
    pub memory_bytes: u64,
    /// Maximum embedding-specific work estimate.
    pub work: u64,
}

/// Version-pinned deterministic RNG identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingRngContract {
    /// Deterministic generator algorithm.
    pub version: &'static str,
    /// Typed substream derivation contract.
    pub derivation: &'static str,
    /// Normalized caller seed.
    pub seed: u64,
}

/// Persistence-neutral embedding invocation, independent of any run UUID.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingInvocationDescriptor {
    /// Closed `analyze(by=...)` catalog value.
    pub catalog_value: &'static str,
    /// Versioned mathematical algorithm contract.
    pub algorithm_version: &'static str,
    /// Normalized graph-native selectors.
    pub selector: EmbeddingProjectionSelector,
    /// Normalized typed options for the selected embedding algorithm.
    pub options: EmbeddingOptions,
    /// Version-pinned deterministic RNG contract.
    pub rng: EmbeddingRngContract,
    /// Actual configured execution limits.
    pub limits: EmbeddingInvocationLimits,
    /// Digest of the exact UUID topology consumed by the kernel.
    pub projection_fingerprint: [u8; 32],
    /// Version of the canonical Arrow result schema.
    pub result_schema_version: &'static str,
}

impl EmbeddingInvocationDescriptor {
    /// Canonical versioned bytes suitable for downstream persistence or hashing.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        text(&mut bytes, "graphforge_embedding_invocation_v1");
        text(&mut bytes, self.catalog_value);
        text(&mut bytes, self.algorithm_version);
        optional_text(&mut bytes, self.selector.label.as_deref());
        optional_text(&mut bytes, self.selector.via.as_deref());
        bytes.push(u8::from(self.selector.directed));
        optional_text(&mut bytes, self.selector.weight.as_deref());
        embedding_options(&mut bytes, &self.options);
        text(&mut bytes, self.rng.version);
        text(&mut bytes, self.rng.derivation);
        bytes.extend_from_slice(&self.rng.seed.to_be_bytes());
        for limit in [
            self.limits.nodes,
            self.limits.adjacency_entries,
            self.limits.output_rows,
            self.limits.iterations,
            self.limits.states,
            self.limits.memory_bytes,
            self.limits.work,
        ] {
            bytes.extend_from_slice(&limit.to_be_bytes());
        }
        bytes.extend_from_slice(&self.projection_fingerprint);
        text(&mut bytes, self.result_schema_version);
        bytes
    }
}

fn embedding_options(output: &mut Vec<u8>, options: &EmbeddingOptions) {
    match options {
        EmbeddingOptions::Node2Vec(options) => {
            text(output, "node2vec");
            usize_value(output, options.dimensions);
            usize_value(output, options.walk_length);
            usize_value(output, options.walks_per_node);
            f64_value(output, options.p);
            f64_value(output, options.q);
            usize_value(output, options.window_size);
            usize_value(output, options.negative_samples);
            usize_value(output, options.epochs);
            f64_value(output, options.learning_rate);
            output.extend_from_slice(&options.seed.to_be_bytes());
        }
        EmbeddingOptions::GraphSage(options) => {
            text(output, "graphsage");
            usize_value(output, options.dimensions);
            usize_value(output, options.hidden_dimensions);
            usize_value(output, options.layers);
            usize_values(output, &options.sample_sizes);
            text(
                output,
                match options.aggregator {
                    GraphSageAggregator::Mean => "mean",
                },
            );
            usize_value(output, options.epochs);
            usize_value(output, options.negative_samples);
            f64_value(output, options.learning_rate);
            texts(output, &options.feature_properties);
            output.extend_from_slice(&options.seed.to_be_bytes());
        }
        EmbeddingOptions::FastRandomProjection(options) => {
            text(output, "fast_random_projection");
            usize_value(output, options.dimensions);
            usize_value(output, options.iteration_weights.len());
            for value in &options.iteration_weights {
                f64_value(output, *value);
            }
            f64_value(output, options.normalization_strength);
            f64_value(output, options.feature_weight);
            texts(output, &options.feature_properties);
            output.extend_from_slice(&options.seed.to_be_bytes());
        }
        EmbeddingOptions::HashGnn(options) => {
            text(output, "hashgnn");
            usize_value(output, options.dimensions);
            usize_value(output, options.iterations);
            f64_value(output, options.embedding_density);
            output.push(u8::from(options.heterogeneous));
            optional_text(output, options.node_type_property.as_deref());
            optional_text(output, options.relationship_type_property.as_deref());
            output.extend_from_slice(&options.seed.to_be_bytes());
        }
    }
}

fn f64_value(output: &mut Vec<u8>, value: f64) {
    output.extend_from_slice(&value.to_bits().to_be_bytes());
}

fn usize_values(output: &mut Vec<u8>, values: &[usize]) {
    usize_value(output, values.len());
    for value in values {
        usize_value(output, *value);
    }
}

fn texts(output: &mut Vec<u8>, values: &[String]) {
    usize_value(output, values.len());
    for value in values {
        text(output, value);
    }
}

/// One Rust-owned result paired with its persistence-neutral invocation.
#[derive(Debug)]
pub struct EmbeddingExecution {
    /// Persistence-neutral description of the completed invocation.
    pub descriptor: EmbeddingInvocationDescriptor,
    /// Canonical Rust-produced Arrow result.
    pub result: RecordBatch,
}

fn text(output: &mut Vec<u8>, value: &str) {
    usize_value(output, value.len());
    output.extend_from_slice(value.as_bytes());
}

fn optional_text(output: &mut Vec<u8>, value: Option<&str>) {
    output.push(u8::from(value.is_some()));
    if let Some(value) = value {
        text(output, value);
    }
}

fn usize_value(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(&(value as u128).to_be_bytes());
}
