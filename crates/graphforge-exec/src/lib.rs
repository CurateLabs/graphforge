//! GraphForge execution session — wires [`graphforge_storage::GraphCatalog`] into a DataFusion
//! [`datafusion::prelude::SessionContext`] and executes [`GraphPlan`]s.
//!
//! Physical CREATE, mutation, traversal, and row operators live in private
//! domain modules. The session module owns planning, read authority, and query
//! evidence lifetime; public result types and exports remain here.
#![forbid(unsafe_code)]
// `name()` returns a string literal but the trait ties it to `&self`.
#![allow(clippy::unnecessary_literal_bound)]

pub mod adjacency;
mod algorithm_analyze;
pub(crate) mod algorithm_analyze_automorphism;
pub(crate) mod algorithm_analyze_automorphism_count;
pub(crate) mod algorithm_analyze_bipartite;
pub(crate) mod algorithm_analyze_bipartite_matching;
pub(crate) mod algorithm_analyze_chromatic_number;
pub(crate) mod algorithm_analyze_conductance;
pub(crate) mod algorithm_analyze_dag_longest_path;
pub(crate) mod algorithm_analyze_dag_longest_path_weighted;
pub(crate) mod algorithm_analyze_dag_topology;
pub(crate) mod algorithm_analyze_dyad_census;
pub(crate) mod algorithm_analyze_edge_coloring;
pub(crate) mod algorithm_analyze_euler;
pub(crate) mod algorithm_analyze_find_cycles;
pub(crate) mod algorithm_analyze_has_euler_circuit;
pub(crate) mod algorithm_analyze_has_euler_path;
pub(crate) mod algorithm_analyze_is_planar;
pub(crate) mod algorithm_analyze_k1_coloring;
pub(crate) mod algorithm_analyze_lowlink;
pub(crate) mod algorithm_analyze_max_cardinality_matching;
pub(crate) mod algorithm_analyze_minimum_k_spanning_tree;
pub(crate) mod algorithm_analyze_minimum_spanning_forest;
pub(crate) mod algorithm_analyze_modularity;
pub(crate) mod algorithm_analyze_node_coloring;
pub(crate) mod algorithm_analyze_transitivity;
pub(crate) mod algorithm_analyze_triad_census;
pub(crate) mod algorithm_analyze_triangle_count;
mod algorithm_cluster;
pub(crate) mod algorithm_cluster_biconnected;
pub(crate) mod algorithm_cluster_hdbscan;
pub(crate) mod algorithm_cluster_kmeans;
pub(crate) mod algorithm_cluster_max_cut;
pub(crate) mod algorithm_cluster_scc;
pub(crate) mod algorithm_cluster_spectral;
pub(crate) mod algorithm_cluster_spinglass;
pub(crate) mod algorithm_cluster_walktrap;
pub(crate) mod algorithm_dispatch;
pub(crate) mod algorithm_embedding_control;
pub(crate) mod algorithm_embedding_fastrp;
pub(crate) mod algorithm_embedding_graphsage;
pub(crate) mod algorithm_embedding_hashgnn;
mod algorithm_embedding_invocation;
pub(crate) mod algorithm_embedding_options;
pub mod mutation;
mod path_hydration;
pub mod read_resource;
pub mod write_resource;
pub use algorithm_embedding_options::validate_embedding_options;
pub use algorithm_graph::AlgorithmProjectionFingerprint;
pub(crate) mod algorithm_arrow_sink;
pub(crate) mod algorithm_embedding_node2vec;
pub(crate) mod algorithm_embedding_output;
pub(crate) mod algorithm_embedding_rng;
pub(crate) mod algorithm_graph;
pub(crate) mod algorithm_k_core;
pub(crate) mod algorithm_matching_blossom;
pub(crate) mod algorithm_matching_state;
pub(crate) mod algorithm_neighbors;
pub(crate) mod algorithm_output;
pub(crate) mod algorithm_partition;
mod algorithm_paths;
pub(crate) mod algorithm_paths_astar;
pub(crate) mod algorithm_paths_bellman_ford;
pub(crate) mod algorithm_paths_delta_stepping;
pub(crate) mod algorithm_paths_dfs;
pub(crate) mod algorithm_paths_dijkstra;
pub(crate) mod algorithm_paths_floyd_warshall;
pub(crate) mod algorithm_paths_gomory_hu;
pub(crate) mod algorithm_paths_max_flow;
pub(crate) mod algorithm_paths_min_cost_flow;
pub(crate) mod algorithm_paths_min_cut;
pub(crate) mod algorithm_paths_min_steiner;
pub(crate) mod algorithm_paths_prize_steiner;
pub(crate) mod algorithm_paths_random_walk;
pub(crate) mod algorithm_paths_steiner;
pub(crate) mod algorithm_paths_transitive_closure;
pub(crate) mod algorithm_paths_yens;
mod algorithm_rank;
mod algorithm_similar;
pub(crate) mod algorithm_similar_jaccard;
pub(crate) mod algorithm_similar_knn;
pub(crate) mod algorithm_weighted_undirected;
#[doc(hidden)]
pub mod demand;
mod edge_count;
mod ordered_one_hop;
mod ordered_two_hop;
pub use crate::adjacency::{
    Adjacency, AdjacencyBacking, AdjacencyProvider, AdjacencyStatus, AdmittedAdjacencyProvider,
    PersistentAdjacencyProvider, ScanBuildAdjacencyProvider,
};
pub use algorithm_analyze::{
    analyze_algorithm, analyze_algorithm_with_compute, analyze_projection_fingerprint,
    embedding_algorithm, embedding_algorithm_execution, embedding_algorithm_execution_with_compute,
    prepare_embedding_invocation_descriptor, prepare_embedding_invocation_descriptor_with_compute,
};
pub use algorithm_cluster::{
    cluster_algorithm, cluster_algorithm_with_compute, cluster_algorithm_with_limits,
    cluster_projection_fingerprint,
};
pub use algorithm_dispatch::AlgorithmLimits;
mod compute_pool;
pub use algorithm_embedding_invocation::{
    EmbeddingExecution, EmbeddingInvocationDescriptor, EmbeddingInvocationLimits,
    EmbeddingProjectionSelector, EmbeddingRngContract,
};
pub use algorithm_paths::{
    paths_algorithm, paths_algorithm_with_compute, paths_projection_fingerprint,
};
pub use algorithm_rank::{
    rank_algorithm, rank_algorithm_with_compute, rank_algorithm_with_limits,
    rank_projection_fingerprint,
};
pub use algorithm_similar::{
    similar_algorithm, similar_algorithm_with_compute, similar_algorithm_with_limits,
    similar_projection_fingerprint,
};
pub use compute_pool::{ComputePool, SharedComputePool};

mod write_driver;

pub use datafusion::physical_plan::SendableRecordBatchStream;

pub use graphforge_core::GfError;
pub use graphforge_ir::GraphPlan;

use arrow::array::Array;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Node property-file stem for the exploratory / untyped catch-all (matches the
/// writer's `UNTYPED_STEM`).
const UNTYPED_STEM: &str = "_untyped";

// ---------------------------------------------------------------------------
// Public result types
// ---------------------------------------------------------------------------

/// Statistics collected during a single query execution.
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// Total number of output rows produced.
    pub rows_produced: u64,
    /// Wall-clock time taken for execution, in milliseconds.
    pub execution_time_ms: u64,
}

/// The openCypher write **side-effect ledger** for one statement (#601/#814):
/// the counters a `Then the side effects should be:` table asserts against.
///
/// `+labels`/`-labels` use label-*token* semantics (a label counts once per new
/// token, not per node) and require a pre-write schema snapshot; they are not
/// computed yet and remain `0` (the conformance harness treats any asserted
/// non-zero label counter as a non-pass — conservative, no false pass).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SideEffects {
    /// Nodes created (`+nodes`).
    pub nodes_created: u64,
    /// Nodes deleted (`-nodes`).
    pub nodes_deleted: u64,
    /// Relationships created (`+relationships`).
    pub relationships_created: u64,
    /// Relationships deleted (`-relationships`).
    pub relationships_deleted: u64,
    /// Property assignments (`+properties`).
    pub properties_set: u64,
    /// Property removals (`-properties`).
    pub properties_removed: u64,
    /// New label tokens (`+labels`); not computed yet (always `0`).
    pub labels_added: u64,
    /// Removed label tokens (`-labels`); not computed yet (always `0`).
    pub labels_removed: u64,
}

/// Graph-native mutation semantics emitted by `graphforge-exec`.
///
/// This closed registry deliberately contains no provenance or knowledge
/// vocabulary. `graphforge-api` is responsible for translating these neutral effects
/// into domain records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MutationKind {
    /// CREATE produced one or more nodes.
    CreateNode,
    /// CREATE produced one or more edges.
    CreateEdge,
    /// MERGE produced one or more graph objects.
    MergeCreate,
    /// MERGE matched existing graph objects without creating one.
    MergeMatchedNoop,
    /// SET assigned one or more properties.
    SetProperty,
    /// REMOVE targeted one or more properties.
    RemoveProperty,
    /// SET added one or more labels.
    AddLabel,
    /// REMOVE removed one or more labels.
    RemoveLabel,
    /// DELETE removed graph objects.
    Delete,
    /// DETACH DELETE removed nodes and any incident edges.
    DetachDelete,
    /// Ontology inference materialized graph facts.
    OntologyInference,
}

/// Graph object kind referenced by a neutral mutation receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MutationSubjectKind {
    /// Public node UUID.
    Node,
    /// Public edge UUID.
    Edge,
}

/// One UUID-referenced graph object in a mutation receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MutationSubject {
    /// Public graph UUID.
    pub uuid: [u8; 16],
    /// Node or edge.
    pub kind: MutationSubjectKind,
}

/// One aggregate semantic effect within a graph statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationEffect {
    /// Closed graph mutation kind.
    pub kind: MutationKind,
    /// Existing objects consumed or matched by the effect.
    pub inputs: Vec<MutationSubject>,
    /// Objects created or changed by the effect.
    pub outputs: Vec<MutationSubject>,
}

/// Deterministically ordered neutral receipt for one successful graph write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MutationReceipt {
    /// Effects ordered by the closed [`MutationKind`] registry.
    pub effects: Vec<MutationEffect>,
}

impl MutationReceipt {
    fn from_accumulators(
        effects: BTreeMap<MutationKind, (HashSet<MutationSubject>, HashSet<MutationSubject>)>,
    ) -> Self {
        let effects = effects
            .into_iter()
            .map(|(kind, (inputs, outputs))| {
                let mut inputs = inputs.into_iter().collect::<Vec<_>>();
                let mut outputs = outputs.into_iter().collect::<Vec<_>>();
                inputs.sort_unstable();
                outputs.sort_unstable();
                MutationEffect {
                    kind,
                    inputs,
                    outputs,
                }
            })
            .collect();
        Self { effects }
    }

    /// Whether the successful statement had no graph mutation effect.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }
}

/// The result of executing a [`GraphPlan`] via an [`ExecutionSession`].
#[derive(Debug)]
pub struct ExecutionResult {
    /// Arrow schema of the result.
    pub schema: SchemaRef,
    /// Output record batches.
    pub batches: Vec<RecordBatch>,
    /// Execution statistics.
    pub stats: ExecutionStats,
    /// For a write statement, the openCypher side-effect counters; `None` for a
    /// read-only query (which has no side effects by construction).
    pub side_effects: Option<SideEffects>,
    /// Neutral graph mutation semantics; absent for reads.
    pub mutation_receipt: Option<MutationReceipt>,
}

impl SideEffects {
    /// Read a single-write summary batch (`GraphCreateExec` / `GraphDeleteExec`
    /// / `GraphSetExec` / `GraphRemoveExec`) into a ledger by column name, so one
    /// reader serves every per-kind path. Absent columns stay `0`.
    #[must_use]
    fn from_summary(batches: &[RecordBatch]) -> Self {
        let mut se = Self::default();
        let Some(b) = batches.first().filter(|b| b.num_rows() > 0) else {
            return se;
        };
        let read = |name: &str| -> u64 {
            b.column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .map_or(0, |a| a.value(0))
        };
        se.nodes_created = read("nodes_created");
        se.relationships_created = read("edges_created");
        se.properties_set = read("properties_set");
        se.labels_added = read("labels_added");
        se.nodes_deleted = read("nodes_deleted");
        se.relationships_deleted = read("edges_deleted");
        se.properties_removed = read("properties_removed");
        se
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn to_df_err(e: GfError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Arrow schema for a read result when execution produced zero batches (so we
/// can still report the correct output schema): derived from the lowered
/// logical plan's `DFSchema`.
fn physical_schema_fallback(logical: &LogicalPlan) -> SchemaRef {
    Arc::new(logical.schema().as_arrow().clone())
}

/// Read a `FixedSizeBinary(16)` cell as a [`Uuid`].
fn fixed_binary_uuid(
    batch: &RecordBatch,
    idx: usize,
    row: usize,
) -> Result<graphforge_core::uuid::Uuid, GfError> {
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .filter(|a| a.value_length() == 16)
        .ok_or_else(|| {
            GfError::Execution(format!("expected FixedSizeBinary(16) at column {idx}"))
        })?;
    // `value()` returns 16 bytes even for a null slot, so guard explicitly — a
    // NULL matched `node_uuid` would otherwise decode to a bogus UUID.
    if arr.is_null(row) {
        return Err(GfError::Execution(format!(
            "matched node_uuid is null at column {idx}"
        )));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(arr.value(row));
    Ok(graphforge_core::uuid::from_bytes(&bytes))
}

/// Borrow a `UInt64` column by index, erroring if the type does not match.
fn u64_column(batch: &RecordBatch, idx: usize) -> Result<&arrow::array::UInt64Array, GfError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .ok_or_else(|| GfError::Execution(format!("expected UInt64 column at index {idx}")))
}

/// Borrow a `Utf8` column by name.
fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a arrow::array::StringArray, GfError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| GfError::Execution(format!("missing column {name}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| GfError::Execution(format!("expected Utf8 column {name}")))
}

/// Small helper: `value` only when the row is non-null.
trait ValueAt {
    fn value_at(&self, i: usize) -> Option<u64>;
}
impl ValueAt for arrow::array::UInt64Array {
    fn value_at(&self, i: usize) -> Option<u64> {
        self.is_valid(i).then(|| self.value(i))
    }
}

// ---------------------------------------------------------------------------
// Send + Sync assertion
// ---------------------------------------------------------------------------

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ExecutionSession>();
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod error_conversion_tests;

mod create_exec;
pub use create_exec::GraphCreateExec;
mod write_exec;
pub use write_exec::{GraphDeleteExec, GraphRemoveExec, GraphSetExec};
mod expand_exec;
pub use expand_exec::{ExpandExec, OntologyInferExec, V4OrdinalIdentityResolver, VarLenExpandExec};
mod row_exec;
pub use row_exec::{OptionalMatchExec, UnwindExec};
mod session;
mod sort_runs;
pub use session::{
    AdjacencyProviderExt, ExecutionSession, GraphForgeQueryPlanner, SessionResourceConfig,
};

pub(crate) use create_exec::{CreateComputed, CreateConfig, CreateTally, eval_create_computed};
use create_exec::{
    CreateExtras, RefNodeCols, build_ref_by_var, validate_edge_specs, write_batch_creates,
};
pub(crate) use expand_exec::V4OrdinalIdentitySession;
use write_exec::{DeleteCol, WriteCol, collect_delete_targets};
pub(crate) use write_exec::{RemoveAccumulator, SetAccumulator};

#[cfg(test)]
mod tests;
