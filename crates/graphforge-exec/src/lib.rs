//! GraphForge execution session — wires [`GraphCatalog`] into a DataFusion
//! [`SessionContext`] and executes [`GraphPlan`]s.
//!
//! # Milestone status
//!
//! - logical-plan lowering #573 — SessionContext wiring
//! - logical-plan lowering #575/#576 — Operator lowering
//! - physical execution #700 — CREATE execution: first physical [`ExecutionPlan`] + custom
//!   [`ExtensionPlanner`]
//! - physical execution #580 — `VarLenExpandExec`: variable-length path expansion (BFS over the
//!   edge table)
//! - physical execution #581 — `OptionalMatchExec`: `OPTIONAL MATCH` left-join with openCypher
//!   null-shaping
//! - physical execution #582 — `UnwindExec`: `UNWIND` list explosion (one row per element)
//!   ← **this issue**
#![forbid(unsafe_code)]
// `name()` returns a string literal but the trait ties it to `&self`.
#![allow(clippy::unnecessary_literal_bound)]

pub mod adjacency;
mod algorithm_analyze;
#[allow(
    dead_code,
    reason = "issue 2106 provides the foundation for exact automorphism counting"
)]
pub(crate) mod algorithm_analyze_automorphism;
#[allow(
    dead_code,
    reason = "issue 2111 provides the exact kernel before handler activation"
)]
pub(crate) mod algorithm_analyze_automorphism_count;
#[allow(
    dead_code,
    reason = "issue 1223 registers the bipartite matching kernel"
)]
pub(crate) mod algorithm_analyze_bipartite;
#[allow(dead_code, reason = "issue 1223 registers the Hopcroft-Karp kernel")]
pub(crate) mod algorithm_analyze_bipartite_matching;
#[allow(dead_code, reason = "issue 1214 registers the chromatic-number kernel")]
pub(crate) mod algorithm_analyze_chromatic_number;
pub(crate) mod algorithm_analyze_conductance;
#[allow(dead_code, reason = "issue 1206 registers the dag-longest-path kernel")]
pub(crate) mod algorithm_analyze_dag_longest_path;
#[allow(
    dead_code,
    reason = "issue 1208 registers the weighted dag-longest-path kernel"
)]
pub(crate) mod algorithm_analyze_dag_longest_path_weighted;
#[allow(dead_code, reason = "issue 1741 adds the shared DAG-family kernel")]
pub(crate) mod algorithm_analyze_dag_topology;
#[allow(dead_code, reason = "issue 1884 registers the dyad-census kernel")]
pub(crate) mod algorithm_analyze_dyad_census;
#[allow(dead_code, reason = "issue 1212 registers the edge-coloring kernel")]
pub(crate) mod algorithm_analyze_edge_coloring;
#[allow(
    dead_code,
    reason = "issue 2104 provides shared Euler construction before leaf activation"
)]
pub(crate) mod algorithm_analyze_euler;
#[allow(dead_code, reason = "issue 1204 registers the find-cycles kernel")]
pub(crate) mod algorithm_analyze_find_cycles;
#[allow(dead_code, reason = "issue 1227 registers the Euler-circuit predicate")]
pub(crate) mod algorithm_analyze_has_euler_circuit;
#[allow(dead_code, reason = "issue 1228 registers the Euler-path predicate")]
pub(crate) mod algorithm_analyze_has_euler_path;
#[allow(dead_code, reason = "issue 1229 registers the planarity predicate")]
pub(crate) mod algorithm_analyze_is_planar;
#[allow(dead_code, reason = "issue 1217 registers the k1-coloring kernel")]
pub(crate) mod algorithm_analyze_k1_coloring;
#[allow(dead_code, reason = "issues 1230 and 1231 register this shared kernel")]
pub(crate) mod algorithm_analyze_lowlink;
#[allow(
    dead_code,
    reason = "issue 1221 registers the maximum-cardinality matching kernel"
)]
pub(crate) mod algorithm_analyze_max_cardinality_matching;
#[allow(dead_code, reason = "#1198 dispatch integration registers this kernel")]
pub(crate) mod algorithm_analyze_minimum_k_spanning_tree;
pub(crate) mod algorithm_analyze_minimum_spanning_forest;
#[allow(dead_code, reason = "issue 1234 registers the modularity kernel")]
pub(crate) mod algorithm_analyze_modularity;
pub(crate) mod algorithm_analyze_node_coloring;
pub(crate) mod algorithm_analyze_transitivity;
#[allow(dead_code, reason = "issue 1885 registers the triad-census kernel")]
pub(crate) mod algorithm_analyze_triad_census;
#[allow(dead_code, reason = "issue 1232 registers the triangle-count kernel")]
pub(crate) mod algorithm_analyze_triangle_count;
mod algorithm_cluster;
pub(crate) mod algorithm_cluster_biconnected;
pub(crate) mod algorithm_cluster_hdbscan;
pub(crate) mod algorithm_cluster_kmeans;
pub(crate) mod algorithm_cluster_max_cut;
#[allow(dead_code)]
pub(crate) mod algorithm_cluster_scc;
pub(crate) mod algorithm_cluster_spectral;
#[allow(dead_code)]
pub(crate) mod algorithm_cluster_spinglass;
pub(crate) mod algorithm_cluster_walktrap;
pub(crate) mod algorithm_dispatch;
#[allow(
    dead_code,
    reason = "embedding kernels consume this shared control foundation"
)]
pub(crate) mod algorithm_embedding_control;
pub(crate) mod algorithm_embedding_fastrp;
pub(crate) mod algorithm_embedding_graphsage;
pub(crate) mod algorithm_embedding_hashgnn;
mod algorithm_embedding_invocation;
#[allow(
    dead_code,
    reason = "embedding leaf dispatches consume the shared typed option boundary"
)]
pub(crate) mod algorithm_embedding_options;
pub use algorithm_embedding_options::validate_embedding_options;
pub use algorithm_graph::AlgorithmProjectionFingerprint;
pub(crate) mod algorithm_arrow_sink;
#[allow(
    dead_code,
    reason = "Node2Vec activation consumes the deterministic walk corpus"
)]
pub(crate) mod algorithm_embedding_node2vec;
#[allow(
    dead_code,
    reason = "embedding kernels consume this canonical output foundation"
)]
pub(crate) mod algorithm_embedding_output;
#[allow(
    dead_code,
    reason = "embedding kernels consume this shared RNG foundation"
)]
pub(crate) mod algorithm_embedding_rng;
pub(crate) mod algorithm_graph;
pub(crate) mod algorithm_k_core;
pub(crate) mod algorithm_matching_blossom;
pub(crate) mod algorithm_matching_state;
pub(crate) mod algorithm_neighbors;
pub(crate) mod algorithm_output;
#[allow(
    dead_code,
    reason = "issues 1223 and 1233 consume the shared partition mapping"
)]
pub(crate) mod algorithm_partition;
mod algorithm_paths;
#[allow(dead_code, reason = "issue 1683 registers this kernel")]
pub(crate) mod algorithm_paths_astar;
#[allow(dead_code, reason = "issue 1691 registers this kernel")]
pub(crate) mod algorithm_paths_bellman_ford;
#[allow(dead_code, reason = "issue 1706 registers this kernel")]
pub(crate) mod algorithm_paths_delta_stepping;
#[allow(
    dead_code,
    reason = "issue 1220 registers the depth-first-search kernel"
)]
pub(crate) mod algorithm_paths_dfs;
#[allow(dead_code, reason = "issue 1665 registers this kernel")]
pub(crate) mod algorithm_paths_dijkstra;
#[allow(dead_code, reason = "issue 1701 registers this kernel")]
pub(crate) mod algorithm_paths_floyd_warshall;
#[allow(
    dead_code,
    reason = "issue 2121 provides the Gomory-Hu kernel before activation"
)]
pub(crate) mod algorithm_paths_gomory_hu;
#[allow(dead_code, reason = "issue 1209 registers both maximum-flow views")]
pub(crate) mod algorithm_paths_max_flow;
#[allow(dead_code, reason = "issue 1213 registers both min-cost flow views")]
pub(crate) mod algorithm_paths_min_cost_flow;
#[allow(dead_code, reason = "issue 1211 registers both minimum-cut views")]
pub(crate) mod algorithm_paths_min_cut;
pub(crate) mod algorithm_paths_min_steiner;
#[allow(
    dead_code,
    reason = "production graph properties normalize to f64; exact scalar variants retain kernel boundary coverage"
)]
pub(crate) mod algorithm_paths_prize_steiner;
#[allow(dead_code, reason = "issue 1222 registers the random-walk kernel")]
pub(crate) mod algorithm_paths_random_walk;
#[allow(
    dead_code,
    reason = "issue 2159 provides Steiner normalization before activation"
)]
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
pub use adjacency::{
    Adjacency, AdjacencyBacking, AdjacencyProvider, AdjacencyStatus, PersistentAdjacencyProvider,
    ScanBuildAdjacencyProvider,
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int8Array, ListBuilder,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, UInt32Array, UInt32Builder,
    UInt64Array, new_null_array,
};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError};
use datafusion::execution::context::{ExecutionProps, QueryPlanner, SessionState};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{
    Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
};
use datafusion::physical_expr::{EquivalenceProperties, create_physical_expr};
pub use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, collect,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;

pub use graphforge_core::GfError;
use graphforge_core::OntologyMode;
pub use graphforge_ir::GraphPlan;
use graphforge_ir::{Direction, ExprArena, ExprId, GraphOp, IrExpr, IrLiteral, VarId};
use graphforge_ontology::OntologyHandle;
use graphforge_plan::{
    DeleteTarget, GraphCreateNode, GraphDeleteNode, GraphRemoveNode, GraphSetNode,
    OptionalMatchNode, RemoveTarget, ResolvedEdgeSpec, ResolvedNodeSpec, SetTarget, UnwindNode,
    VarLenExpandNode,
};
use graphforge_rel::{GraphPlanLowerer, scalar_to_ir_literal};
use graphforge_storage::GraphCatalog;

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

/// Whether `plan` contains an operator that reads persisted topology/property
/// data (and therefore needs a real project directory to bind its Parquet
/// provider). Recurses into `Optional`/`Union` sub-plans.
fn plan_reads_persisted_data(plan: &GraphPlan) -> bool {
    use graphforge_ir::GraphOp;
    plan.ops.iter().any(|op| match op {
        GraphOp::NodeScan { .. }
        | GraphOp::EdgeScan { .. }
        | GraphOp::TypedEdgeScan { .. }
        | GraphOp::Expand { .. } => true,
        GraphOp::Optional { child }
        | GraphOp::Exists { child, .. }
        | GraphOp::PatternComprehension { child, .. }
        | GraphOp::ListElementPatternComprehension { child, .. } => {
            plan_reads_persisted_data(child)
        }
        GraphOp::Union { inputs, .. } => inputs.iter().any(plan_reads_persisted_data),
        _ => false,
    })
}

/// True when `plan` contains a write terminal that must lower via
/// [`GraphPlanLowerer::new_for_writes`].
fn plan_requires_writes(plan: &GraphPlan) -> bool {
    use graphforge_ir::GraphOp;
    plan.ops.iter().any(|op| match op {
        GraphOp::Create { .. }
        | GraphOp::Merge { .. }
        | GraphOp::Delete { .. }
        | GraphOp::Set { .. }
        | GraphOp::Remove { .. } => true,
        GraphOp::Optional { child }
        | GraphOp::Exists { child, .. }
        | GraphOp::PatternComprehension { child, .. }
        | GraphOp::ListElementPatternComprehension { child, .. } => plan_requires_writes(child),
        GraphOp::Union { inputs, .. } => inputs.iter().any(plan_requires_writes),
        _ => false,
    })
}

// ---------------------------------------------------------------------------
// GraphCreateExec — physical node for CREATE
// ---------------------------------------------------------------------------

/// Physical execution node for `CREATE`.
///
/// Drives a [`graphforge_storage::GraphWriter`] **once per input row** (#703): for each
/// row it references MATCH-bound vars by reading their identity from the input
/// columns (`ResolvedNodeSpec::is_reference`) and mints fresh UUIDv7s for the
/// new nodes/edges. A standalone `CREATE` is driven by the implicit single unit
/// row (so it creates exactly once). Emits a one-row summary batch
/// (`nodes_created` / `edges_created`) of the totals across all rows.
pub struct GraphCreateExec {
    /// Input plan whose rows drive the writes (one CREATE per row, #703). A
    /// standalone CREATE is driven by the implicit single unit row.
    input: Arc<dyn ExecutionPlan>,
    nodes: Vec<ResolvedNodeSpec>,
    edges: Vec<ResolvedEdgeSpec>,
    /// For each **reference** node spec (a MATCH-bound var), the input column
    /// indices of its `node_uuid` / `node_id` — resolved by qualified name
    /// `var_<n>.…` so a multi-`node_uuid` input picks the right one.
    ref_cols: Vec<RefNodeCols>,
    /// The input's logical schema, used to build physical exprs for any
    /// row-dependent computed property values (#814).
    in_df_schema: DFSchemaRef,
    dir: PathBuf,
    mode: OntologyMode,
    semantic_composition_fingerprint: Option<String>,
    schema: SchemaRef,
    /// `true` when this node emits created-entity rows (write-result RETURN);
    /// `false` for the one-row write summary (#814).
    emit_rows: bool,
    /// In emit-rows mode the output relation carries the created rows, not the
    /// summary, so the side-effect counts ride this shared tally instead — read
    /// back by `execute_create` after execution by walking the physical plan.
    effects: Arc<std::sync::Mutex<CreateTally>>,
    props: Arc<PlanProperties>,
}

/// Resolved input-column locations for one referenced (MATCH-bound) node var.
#[derive(Clone)]
struct RefNodeCols {
    var: u32,
    uuid_idx: usize,
    uuid_child_idx: Option<usize>,
    node_id_idx: Option<usize>,
}

impl RefNodeCols {
    /// Resolve a referenced var's identity columns (`var_<n>.node_uuid` /
    /// `var_<n>.node_id`) from a logical schema, `None` when unbound.
    fn resolve(schema: &DFSchema, var: u32) -> Option<Self> {
        Self::resolve_with_alias(schema, var, &format!("var_{var}"))
    }

    fn resolve_with_alias(schema: &DFSchema, var: u32, alias: &str) -> Option<Self> {
        let qual = datafusion::common::TableReference::bare(alias);
        Self::resolve_qualified(schema, var, &qual)
            .or_else(|| Self::resolve_unqualified(schema, var))
            .or_else(|| Self::resolve_struct(schema, var, alias))
    }

    fn resolve_qualified(
        schema: &DFSchema,
        var: u32,
        qual: &datafusion::common::TableReference,
    ) -> Option<Self> {
        Some(Self {
            var,
            uuid_idx: schema.index_of_column_by_name(Some(qual), "node_uuid")?,
            uuid_child_idx: None,
            node_id_idx: Some(schema.index_of_column_by_name(Some(qual), "node_id")?),
        })
    }

    fn resolve_unqualified(schema: &DFSchema, var: u32) -> Option<Self> {
        Some(Self {
            var,
            uuid_idx: schema.index_of_column_by_name(None, "node_uuid")?,
            uuid_child_idx: None,
            node_id_idx: Some(schema.index_of_column_by_name(None, "node_id")?),
        })
    }

    fn resolve_struct(schema: &DFSchema, var: u32, alias: &str) -> Option<Self> {
        let uuid_idx = schema.index_of_column_by_name(None, alias)?;
        let DataType::Struct(fields) = schema.field(uuid_idx).data_type() else {
            return None;
        };
        let uuid_child_idx = fields.iter().position(|field| field.name() == "node_uuid");
        if uuid_child_idx.is_none() && !dynamic_struct_contains_node(fields) {
            return None;
        }
        Some(Self {
            var,
            uuid_idx,
            uuid_child_idx,
            node_id_idx: None,
        })
    }

    fn resolve_struct_at(schema: &DFSchema, var: u32, uuid_idx: usize) -> Option<Self> {
        let DataType::Struct(fields) = schema.field(uuid_idx).data_type() else {
            return None;
        };
        let uuid_child_idx = fields.iter().position(|field| field.name() == "node_uuid");
        if uuid_child_idx.is_none() && !dynamic_struct_contains_node(fields) {
            return None;
        }
        Some(Self {
            var,
            uuid_idx,
            uuid_child_idx,
            node_id_idx: None,
        })
    }
}

fn dynamic_struct_contains_node(fields: &arrow::datatypes::Fields) -> bool {
    fields.iter().any(|field| {
        field.name().starts_with("__het_value_")
            && matches!(field.data_type(), DataType::Struct(value_fields)
                if value_fields.iter().any(|value_field| value_field.name() == "node_uuid"))
    })
}

impl GraphCreateExec {
    /// Build a physical CREATE node from its logical counterpart and planned
    /// input.
    #[must_use]
    pub fn new(node: &GraphCreateNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let emit_rows = node.emits_rows();
        // Summary mode → the fixed write-summary schema; emit-rows mode → the
        // created-entity row schema the logical node declares.
        let schema: SchemaRef = if emit_rows {
            node.schema().inner().clone()
        } else {
            GraphCreateNode::summary_schema()
        };
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        // Resolve, per reference spec, the input columns carrying the matched
        // node's identity (qualified `var_<n>.node_uuid` / `var_<n>.node_id`).
        let in_schema = node.input.schema();
        let mut used_struct_columns = HashSet::new();
        let ref_cols = node
            .nodes
            .iter()
            .filter(|n| n.is_reference)
            .filter_map(|n| {
                RefNodeCols::resolve(in_schema, n.var).or_else(|| {
                    (0..in_schema.fields().len()).find_map(|index| {
                        if used_struct_columns.contains(&index) {
                            return None;
                        }
                        let resolved = RefNodeCols::resolve_struct_at(in_schema, n.var, index)?;
                        used_struct_columns.insert(index);
                        Some(resolved)
                    })
                })
            })
            .collect();
        Self {
            input,
            nodes: node.nodes.clone(),
            edges: node.edges.clone(),
            ref_cols,
            in_df_schema: in_schema.clone(),
            dir: node.dir.clone(),
            mode: node.mode,
            semantic_composition_fingerprint: node.semantic_composition_fingerprint.clone(),
            schema,
            emit_rows,
            effects: Arc::new(std::sync::Mutex::new(CreateTally::default())),
            props,
        }
    }

    /// Read back the accumulated side-effect tally (emit-rows mode), for
    /// `execute_create` to build the ledger after execution.
    pub(crate) fn effects(&self) -> CreateTally {
        self.effects.lock().map(|t| *t).unwrap_or_default()
    }

    /// Whether this exec emits created-entity rows (vs the summary).
    #[must_use]
    pub fn emits_rows(&self) -> bool {
        self.emit_rows
    }

    /// Owned config for [`run_creates`] so the writes can run in a `'static`
    /// future without borrowing the exec node.
    fn config(&self) -> CreateConfig {
        CreateConfig {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            ref_cols: self.ref_cols.clone(),
            in_df_schema: self.in_df_schema.clone(),
            dir: self.dir.clone(),
            mode: self.mode,
            semantic_composition_fingerprint: self.semantic_composition_fingerprint.clone(),
            out_schema: self.schema.clone(),
        }
    }
}

impl fmt::Debug for GraphCreateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphCreateExec {{ nodes: {}, edges: {} }}",
            self.nodes.len(),
            self.edges.len()
        )
    }
}

impl DisplayAs for GraphCreateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphCreateExec: nodes={}, edges={}",
            self.nodes.len(),
            self.edges.len()
        )
    }
}

impl ExecutionPlan for GraphCreateExec {
    fn name(&self) -> &str {
        "GraphCreateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("GraphCreateExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            ref_cols: self.ref_cols.clone(),
            in_df_schema: self.in_df_schema.clone(),
            dir: self.dir.clone(),
            mode: self.mode,
            semantic_composition_fingerprint: self.semantic_composition_fingerprint.clone(),
            schema: self.schema.clone(),
            emit_rows: self.emit_rows,
            // Share the SAME tally so `execute_create` can read counts back after
            // execution regardless of optimizer cloning.
            effects: Arc::clone(&self.effects),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        use futures::StreamExt;

        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraphCreateExec only has partition 0, got {partition}"
            )));
        }
        // The CREATE runs once per input row. Drain the child **incrementally**
        // (batch-by-batch) rather than collecting the whole frontier first, so a
        // large MATCH/UNWIND input isn't materialized in memory (#747). The
        // GraphWriter buffers created rows and is flushed once at the end, so
        // counts/semantics are identical to the collected path.
        let input = self.input.clone();
        let cfg = self.config();
        let schema = self.schema.clone();
        let out_schema = self.schema.clone();
        let emit_rows = self.emit_rows;
        let effects = Arc::clone(&self.effects);
        let fut = async move {
            // Validate edge shapes once, before opening the writer.
            validate_edge_specs(&cfg).map_err(to_df_err)?;
            let ref_by_var = build_ref_by_var(&cfg);
            let persisted_ids = cfg
                .ref_cols
                .iter()
                .any(|cols| cols.node_id_idx.is_none())
                .then(|| persisted_node_ids(&cfg.dir))
                .transpose()
                .map_err(to_df_err)?;
            let mut writer = graphforge_storage::GraphWriter::open(&cfg.dir, cfg.mode)
                .map_err(to_df_err)?
                .with_semantic_composition_fingerprint(
                    cfg.semantic_composition_fingerprint.clone(),
                );
            let mut tally = CreateTally::default();
            let mut emitted: Vec<RecordBatch> = Vec::new();

            // Partition-safe drive: `execute_stream` coalesces a multi-partition
            // child into one stream (0 → empty, 1 → execute(0), 2.. →
            // CoalescePartitionsExec) — the same shape `collect` used, so no rows
            // are dropped. (A bare `input.execute(0, …)` would read only
            // partition 0.)
            let mut stream = datafusion::physical_plan::execute_stream(input, context)?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                // Evaluate any row-dependent property values against this batch
                // (#814), then mint per row, merging them in.
                let computed = eval_create_computed(&cfg, &batch)?;
                if emit_rows {
                    // Write-result RETURN: mint and emit the created-entity rows.
                    emitted.push(
                        emit_batch_creates(
                            &cfg,
                            &mut writer,
                            &batch,
                            &computed,
                            &ref_by_var,
                            persisted_ids.as_ref(),
                            &mut tally,
                        )
                        .map_err(to_df_err)?,
                    );
                } else {
                    write_batch_creates(
                        &cfg,
                        &mut writer,
                        &batch,
                        &ref_by_var,
                        CreateExtras {
                            computed: Some(&computed),
                            persisted_ids: persisted_ids.as_ref(),
                            ..CreateExtras::default()
                        },
                        &mut tally,
                    )
                    .map_err(to_df_err)?;
                }
            }

            tally.labels_added = distinct_created_labels(&cfg.nodes, tally.nodes_created);
            writer.flush().map_err(to_df_err)?;
            if emit_rows {
                // The result relation is the created rows; the side-effect counts
                // ride the shared tally for `execute_create` to read back.
                if let Ok(mut slot) = effects.lock() {
                    *slot = tally;
                }
                if emitted.is_empty() {
                    return Ok(RecordBatch::new_empty(out_schema));
                }
                arrow::compute::concat_batches(&out_schema, &emitted)
                    .map_err(|e| to_df_err(GfError::Execution(e.to_string())))
            } else {
                summary_batch(&cfg.out_schema, &tally).map_err(to_df_err)
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut),
        )))
    }
}

/// Owned configuration for [`run_creates`].
pub(crate) struct CreateConfig {
    pub(crate) nodes: Vec<ResolvedNodeSpec>,
    pub(crate) edges: Vec<ResolvedEdgeSpec>,
    ref_cols: Vec<RefNodeCols>,
    /// Input logical schema, for building physical exprs from the specs'
    /// row-dependent computed property values (#814).
    pub(crate) in_df_schema: DFSchemaRef,
    dir: PathBuf,
    mode: OntologyMode,
    semantic_composition_fingerprint: Option<String>,
    out_schema: SchemaRef,
}

/// Per-batch row-dependent CREATE property values, keyed by the spec's `var`:
/// each entry is the `(prop_name, evaluated column)` pairs for that var, read at
/// the row index being minted (#814).
pub(crate) type CreateComputed = HashMap<u32, Vec<(String, arrow::array::ArrayRef)>>;

/// Evaluate each spec's row-dependent computed property exprs against `batch`,
/// producing the per-var columns the create writer merges per minted row (#814).
///
/// The exprs were lowered against the input's logical schema at planning time;
/// here they convert to physical exprs (`cfg.in_df_schema`) and evaluate over
/// the batch — mirroring the SET path's per-batch value evaluation.
pub(crate) fn eval_create_computed(
    cfg: &CreateConfig,
    batch: &RecordBatch,
) -> Result<CreateComputed, DataFusionError> {
    let props = ExecutionProps::new();
    let mut out: CreateComputed = HashMap::new();
    let mut eval = |var: u32, name: &str, expr: &datafusion::logical_expr::Expr| {
        let phys = create_physical_expr(expr, &cfg.in_df_schema, &props)?;
        let array = phys.evaluate(batch)?.into_array(batch.num_rows())?;
        out.entry(var).or_default().push((name.to_owned(), array));
        Ok::<(), DataFusionError>(())
    };
    for n in &cfg.nodes {
        for (name, expr) in &n.computed_properties {
            eval(n.var, name, expr)?;
        }
    }
    for e in &cfg.edges {
        for (name, expr) in &e.computed_properties {
            eval(e.var, name, expr)?;
        }
    }
    Ok(out)
}

/// Reject unsupported edge shapes (independent of input rows). Run once before
/// touching the writer.
fn validate_edge_specs(cfg: &CreateConfig) -> Result<(), GfError> {
    for spec in &cfg.edges {
        // Edge properties are persisted as of #784 (write to
        // `edge_properties/<REL>.parquet` keyed by edge_uuid below). They are
        // routed by relation name, and the read-side join resolves that same
        // name — so an edge with properties but no relation type would write
        // its props under `_untyped` where no MATCH can ever read them back.
        // Reject it loudly rather than silently orphaning the data. (A typed
        // edge is the only well-formed shape: openCypher CREATE names a single
        // relation type.)
        if !spec.properties.is_empty() && spec.rel_type_name.is_none() {
            return Err(GfError::Execution(
                "CREATE: an edge with properties must have a relationship type \
                 (e.g. `-[:KNOWS {since: 2020}]->`)"
                    .into(),
            ));
        }
        if matches!(spec.direction, graphforge_ir::Direction::Undirected) {
            return Err(GfError::Execution(
                "CREATE: undirected edges are not supported; use a directed edge (-> or <-)".into(),
            ));
        }
    }
    Ok(())
}

/// Per-reference-var lookup of its `(uuid_idx, node_id_idx)` input columns.
fn build_ref_by_var(cfg: &CreateConfig) -> std::collections::HashMap<u32, &RefNodeCols> {
    cfg.ref_cols.iter().map(|r| (r.var, r)).collect()
}

fn persisted_node_ids(dir: &Path) -> Result<std::collections::HashMap<[u8; 16], u64>, GfError> {
    let mut ids = std::collections::HashMap::new();
    for batch in graphforge_storage::read_nodes(dir).map_err(|e| GfError::Storage(e.to_string()))? {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Storage("nodes file missing node_uuid".into()))?;
        let node_ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| GfError::Storage("nodes file missing node_id".into()))?;
        for row in 0..batch.num_rows() {
            let uuid: [u8; 16] = uuids
                .value(row)
                .try_into()
                .map_err(|_| GfError::Storage("node_uuid must contain exactly 16 bytes".into()))?;
            ids.insert(uuid, node_ids.value(row));
        }
    }
    Ok(ids)
}

fn referenced_node_uuid(
    batch: &RecordBatch,
    cols: &RefNodeCols,
    row: usize,
) -> Result<graphforge_core::uuid::Uuid, GfError> {
    let parent = batch.column(cols.uuid_idx);
    if parent.is_null(row) {
        return Err(GfError::Execution(format!(
            "matched node_uuid is null for var {}",
            cols.var
        )));
    }
    let array = if let Some(child_idx) = cols.uuid_child_idx {
        parent
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| GfError::Execution("CREATE node reference is not a struct".into()))?
            .column(child_idx)
    } else if cols.node_id_idx.is_none() {
        let tagged = parent
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| GfError::Execution("CREATE node reference is not a struct".into()))?;
        let tag = tagged
            .column_by_name("__het_tag")
            .and_then(|column| column.as_any().downcast_ref::<Int8Array>())
            .ok_or_else(|| GfError::Execution("CREATE node reference has no type tag".into()))?
            .value(row);
        let variant = tagged
            .column_by_name(&format!("__het_value_{tag}"))
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| {
                GfError::Execution("CREATE node reference variant is not a node".into())
            })?;
        variant
            .column_by_name("node_uuid")
            .ok_or_else(|| GfError::Execution("CREATE node reference has no node_uuid".into()))?
    } else {
        parent
    };
    let uuids = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| GfError::Execution("CREATE node_uuid is not fixed binary".into()))?;
    if uuids.is_null(row) {
        return Err(GfError::Execution(format!(
            "matched node_uuid is null for var {}",
            cols.var
        )));
    }
    Ok(graphforge_core::uuid::from_bytes(
        uuids.value(row).try_into().map_err(|_| {
            GfError::Execution("CREATE node_uuid must contain exactly 16 bytes".into())
        })?,
    ))
}

/// The one-row write summary `{nodes_created, edges_created}`.
fn summary_batch(schema: &SchemaRef, tally: &CreateTally) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![tally.nodes_created])),
            Arc::new(UInt64Array::from(vec![tally.edges_created])),
            Arc::new(UInt64Array::from(vec![tally.properties_set])),
            Arc::new(UInt64Array::from(vec![tally.labels_added])),
        ],
    )
    .map_err(|e| GfError::Execution(e.to_string()))
}

/// Running CREATE side-effect tallies accumulated across input batches (#601):
/// minted nodes, minted edges, non-null property assignments, and new labels.
#[derive(Default, Clone, Copy)]
pub(crate) struct CreateTally {
    pub nodes_created: u64,
    pub edges_created: u64,
    pub properties_set: u64,
    pub labels_added: u64,
}

/// `+labels` for a CREATE: the count of distinct label *tokens* applied to minted
/// nodes (openCypher label-token semantics — a label counts once however many
/// nodes carry it). Counts distinct label ids (the binder interns each label
/// name to one `TypeId`, so this equals distinct names — and, unlike label names,
/// resolves in exploratory mode where the name lives only in the runtime catalog).
/// Assumes an empty pre-existing label set (correct for the `Given an empty graph`
/// scenarios); reuse of an already-present label would over-count, so such a
/// scenario fails rather than passing — never a false pass. 0 when nothing minted.
pub(crate) fn distinct_created_labels(nodes: &[ResolvedNodeSpec], nodes_created: u64) -> u64 {
    if nodes_created == 0 {
        return 0;
    }
    nodes
        .iter()
        .filter(|n| !n.is_reference)
        .flat_map(|n| n.label_ids.iter().copied())
        .collect::<HashSet<u32>>()
        .len() as u64
}

/// Driver-supplied extras for the create phase (#792): reject references to
/// entities deleted earlier in the statement, and record minted identities so
/// the driver can extend its frontier. The `Default` is the single-clause
/// CREATE shape (nothing deleted, no recording).
#[derive(Default)]
struct CreateExtras<'a> {
    deleted: Option<&'a HashSet<[u8; 16]>>,
    recorder: Option<&'a mut write_driver::CreateRecorder>,
    /// Per-var row-dependent property columns (evaluated by
    /// [`eval_create_computed`]); merged into each minted entity's props at its
    /// row index (#814).
    computed: Option<&'a CreateComputed>,
    persisted_ids: Option<&'a HashMap<[u8; 16], u64>>,
}

/// Count the non-null property assignments in `props`: a null value is an
/// absent property in openCypher, so it is not a `+properties` side effect.
fn count_set_props(props: &std::collections::HashMap<String, graphforge_ir::IrLiteral>) -> u64 {
    props
        .values()
        .filter(|v| !matches!(v, graphforge_ir::IrLiteral::Null))
        .count() as u64
}

/// Merge the row-dependent computed property values for `var` (at `row`) into
/// `props` (#814). A null value omits the property — openCypher treats a
/// property set to null as absent, matching the SET path.
fn merge_computed(
    computed: Option<&CreateComputed>,
    var: u32,
    row: usize,
    props: &mut std::collections::HashMap<String, graphforge_ir::IrLiteral>,
) -> Result<(), GfError> {
    let Some(cols) = computed.and_then(|m| m.get(&var)) else {
        return Ok(());
    };
    for (name, array) in cols {
        let scalar = ScalarValue::try_from_array(array, row)
            .map_err(|e| GfError::Execution(e.to_string()))?;
        if scalar.is_null() {
            continue;
        }
        let lit = scalar_to_ir_literal(&scalar).map_err(|e| GfError::Execution(e.to_string()))?;
        props.insert(name.clone(), lit);
    }
    Ok(())
}

/// Apply the CREATE to one input batch, **once per row** (#703): reference
/// MATCH-bound vars by reading their identity from the row, mint new vars +
/// edges per row, accumulating the running totals. The writer is opened by the
/// caller and shared across batches (so a streamed input flushes only once).
#[allow(
    clippy::too_many_lines,
    reason = "node and edge creation share per-row endpoint and recorder state"
)]
fn write_batch_creates(
    cfg: &CreateConfig,
    writer: &mut graphforge_storage::GraphWriter,
    batch: &RecordBatch,
    ref_by_var: &std::collections::HashMap<u32, &RefNodeCols>,
    mut extras: CreateExtras<'_>,
    tally: &mut CreateTally,
) -> Result<(), GfError> {
    use std::collections::HashMap;

    use graphforge_core::uuid::{Uuid, new_v7, to_bytes};

    let exec_err = |m: String| GfError::Execution(m);

    for row in 0..batch.num_rows() {
        // Per-ROW var→uuid binding: each matched row references its own matched
        // nodes and mints its own new nodes/edges.
        let mut var_to_uuid: HashMap<u32, Uuid> = HashMap::new();

        for spec in &cfg.nodes {
            if spec.is_reference {
                // Referenced (MATCH-bound or earlier-created) node: read
                // identity from the row, register it so edges resolve — do NOT
                // write or count it.
                let cols = ref_by_var.get(&spec.var).ok_or_else(|| {
                    exec_err(format!(
                        "CREATE references var {} not found in the input schema {:?}",
                        spec.var,
                        batch.schema()
                    ))
                })?;
                let uuid = referenced_node_uuid(batch, cols, row)?;
                if extras.deleted.is_some_and(|d| d.contains(&to_bytes(&uuid))) {
                    return Err(exec_err(
                        "CREATE references an entity deleted earlier in this statement".into(),
                    ));
                }
                let node_id = if let Some(node_id_idx) = cols.node_id_idx {
                    u64_column(batch, node_id_idx)?
                        .value_at(row)
                        .ok_or_else(|| {
                            exec_err(format!("matched node_id is null for var {}", spec.var))
                        })?
                } else {
                    writer
                        .node_id_for_uuid(&uuid)
                        .or_else(|| {
                            extras
                                .persisted_ids
                                .and_then(|ids| ids.get(&to_bytes(&uuid)))
                                .copied()
                        })
                        .ok_or_else(|| {
                            exec_err(format!(
                                "CREATE references node {uuid} absent from persisted topology"
                            ))
                        })?
                };
                writer.register_existing_node(uuid, node_id)?;
                var_to_uuid.insert(spec.var, uuid);
            } else {
                let uuid = new_v7();
                let (type_ids, type_id) = resolved_node_labels(spec);
                let node_id = writer.create_node_with_labels(uuid, &type_ids)?;
                var_to_uuid.insert(spec.var, uuid);
                let mut props: HashMap<String, graphforge_ir::IrLiteral> =
                    spec.properties.iter().cloned().collect();
                merge_computed(extras.computed, spec.var, row, &mut props)?;
                props.retain(|_, value| !matches!(value, graphforge_ir::IrLiteral::Null));
                tally.properties_set += count_set_props(&props);
                if !props.is_empty() {
                    writer.set_properties(
                        &uuid,
                        spec.label_names.first().map(String::as_str),
                        props,
                    )?;
                }
                if let Some(rec) = extras.recorder.as_deref_mut() {
                    rec.record_node(spec.var, to_bytes(&uuid), node_id, type_id.0);
                }
                tally.nodes_created += 1;
            }
        }

        for spec in &cfg.edges {
            let src = *var_to_uuid.get(&spec.src).ok_or_else(|| {
                exec_err(format!(
                    "CREATE edge references unbound src var {}",
                    spec.src
                ))
            })?;
            let dst = *var_to_uuid.get(&spec.dst).ok_or_else(|| {
                exec_err(format!(
                    "CREATE edge references unbound dst var {}",
                    spec.dst
                ))
            })?;
            // Honor arrow orientation: src/dst are in pattern order, so a
            // reversed arrow `(a)<-[:R]-(b)` persists as b->a.
            let (storage_src, storage_dst) = match spec.direction {
                graphforge_ir::Direction::In => (dst, src),
                _ => (src, dst),
            };
            let rel_name = spec.rel_type_name.as_deref().unwrap_or("_UNKNOWN");
            let edge_uuid = new_v7();
            // Edge properties (#784) are routed by relation name so the
            // read-side join resolves them. `confidence` has no special meaning.
            let mut props: HashMap<String, graphforge_ir::IrLiteral> =
                spec.properties.iter().cloned().collect();
            merge_computed(extras.computed, spec.var, row, &mut props)?;
            props.retain(|_, value| !matches!(value, graphforge_ir::IrLiteral::Null));
            writer.create_edge(edge_uuid, rel_name, &storage_src, &storage_dst)?;
            tally.properties_set += count_set_props(&props);
            if !props.is_empty() {
                writer.set_edge_properties(&edge_uuid, spec.rel_type_name.as_deref(), props)?;
            }
            if let Some(rec) = extras.recorder.as_deref_mut() {
                rec.record_edge(
                    spec.var,
                    to_bytes(&edge_uuid),
                    to_bytes(&storage_src),
                    to_bytes(&storage_dst),
                    spec.rel_type_name.clone(),
                );
            }
            tally.edges_created += 1;
        }
    }
    Ok(())
}

fn resolved_node_labels(
    spec: &ResolvedNodeSpec,
) -> (Vec<graphforge_core::TypeId>, graphforge_core::TypeId) {
    let labels: Vec<_> = spec
        .label_ids
        .iter()
        .copied()
        .map(graphforge_core::TypeId)
        .collect();
    let primary = labels
        .first()
        .copied()
        .unwrap_or(graphforge_core::TypeId(u32::MAX));
    (labels, primary)
}

/// Emit-rows CREATE (#814 write-result RETURN): run the same writer path as
/// summary CREATE, then build the output batch = input columns (passed through)
/// plus each freshly-created node's identity/property columns. Reference nodes
/// arrive through passthrough input columns; created edges are written and
/// counted but emit no result columns.
fn emit_batch_creates(
    cfg: &CreateConfig,
    writer: &mut graphforge_storage::GraphWriter,
    batch: &RecordBatch,
    computed: &CreateComputed,
    ref_by_var: &std::collections::HashMap<u32, &RefNodeCols>,
    persisted_ids: Option<&HashMap<[u8; 16], u64>>,
    tally: &mut CreateTally,
) -> Result<RecordBatch, GfError> {
    let n = batch.num_rows();
    let mut recorder = write_driver::CreateRecorder::default();
    write_batch_creates(
        cfg,
        writer,
        batch,
        ref_by_var,
        CreateExtras {
            recorder: Some(&mut recorder),
            computed: Some(computed),
            persisted_ids,
            ..CreateExtras::default()
        },
        tally,
    )?;

    let mut out_cols: Vec<ArrayRef> = batch.columns().to_vec();
    for spec in cfg.nodes.iter().filter(|s| !s.is_reference) {
        append_created_node_output_cols(spec, n, computed, &recorder, &mut out_cols)?;
    }
    RecordBatch::try_new(cfg.out_schema.clone(), out_cols)
        .map_err(|e| GfError::Execution(e.to_string()))
}

fn append_created_node_output_cols(
    spec: &ResolvedNodeSpec,
    rows: usize,
    computed: &CreateComputed,
    recorder: &write_driver::CreateRecorder,
    out_cols: &mut Vec<ArrayRef>,
) -> Result<(), GfError> {
    use arrow::array::{FixedSizeBinaryBuilder, UInt32Array};

    let empty_uuids: &[[u8; 16]] = &[];
    let empty_node_ids: &[u64] = &[];
    let empty_type_ids: &[u32] = &[];
    let (uuids, node_ids, type_ids) = match recorder.node_identities(spec.var) {
        Some(identities) => identities,
        None if rows == 0 => (empty_uuids, empty_node_ids, empty_type_ids),
        None => {
            return Err(GfError::Execution(format!(
                "emit-rows CREATE did not record identities for var {}",
                spec.var
            )));
        }
    };
    if uuids.len() != rows || node_ids.len() != rows || type_ids.len() != rows {
        return Err(GfError::Execution(format!(
            "created var {} has incomplete emitted identities",
            spec.var
        )));
    }

    let mut uuid_b = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for uuid in uuids {
        uuid_b
            .append_value(uuid)
            .map_err(|e| GfError::Execution(e.to_string()))?;
    }
    out_cols.push(Arc::new(uuid_b.finish()));
    out_cols.push(Arc::new(UInt64Array::from(node_ids.to_vec())));
    out_cols.push(Arc::new(UInt32Array::from(type_ids.to_vec())));
    out_cols.push(write_driver::repeated_label_sets(&spec.label_ids, rows));

    for (_, lit) in &spec.properties {
        let scalar = graphforge_rel::expr::ir_literal_to_scalar(lit);
        out_cols.push(
            scalar
                .to_array_of_size(rows)
                .map_err(|e| GfError::Execution(e.to_string()))?,
        );
    }
    if let Some(cols) = computed.get(&spec.var) {
        for (_, arr) in cols {
            if arr.len() != rows {
                return Err(GfError::Execution(format!(
                    "computed property column for var {} has {} rows, expected {rows}",
                    spec.var,
                    arr.len()
                )));
            }
            out_cols.push(Arc::clone(arr));
        }
    }
    Ok(())
}

/// Walk a physical plan for the emit-rows [`GraphCreateExec`]'s accumulated
/// side-effect tally (#814). In emit-rows mode the output relation carries the
/// created rows, not the summary, so the counts are read back from the exec's
/// shared tally after execution rather than from the result batch.
fn create_tally_in_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<CreateTally> {
    if let Some(c) = plan.downcast_ref::<GraphCreateExec>()
        && c.emits_rows()
    {
        return Some(c.effects());
    }
    plan.children().into_iter().find_map(create_tally_in_plan)
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

// ---------------------------------------------------------------------------
// GraphDeleteExec — physical node for DELETE / DETACH DELETE
// ---------------------------------------------------------------------------

/// Resolved input-column location of one delete target's identity column.
#[derive(Clone)]
struct DeleteCol {
    /// Column index of `var_<n>.node_uuid` (node target) or `var_<n>.edge_uuid`
    /// (edge target) within the input schema.
    uuid_idx: usize,
    is_edge: bool,
}

/// Physical execution node for `DELETE` / `DETACH DELETE` (#740).
///
/// Drains its input (the preceding `MATCH`), collecting the matched entities'
/// UUIDs from each row, then rewrites the affected Parquet files via the
/// [`graphforge_storage::mutator`] primitives. Emits a one-row summary batch
/// (`nodes_deleted` / `edges_deleted`).
///
/// openCypher semantics: deleting a node that still has relationships **without**
/// `DETACH` is an execution error; `DETACH DELETE` also removes the node's
/// incident edges.
pub struct GraphDeleteExec {
    input: Arc<dyn ExecutionPlan>,
    /// Per delete target, the input column carrying its identity UUID.
    cols: Vec<DeleteCol>,
    detach: bool,
    dir: PathBuf,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl GraphDeleteExec {
    /// Build the physical DELETE node from its logical counterpart and input.
    #[must_use]
    pub fn new(node: &GraphDeleteNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = GraphDeleteNode::summary_schema();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        // Resolve, per target, the input column carrying its identity UUID
        // (qualified `var_<n>.node_uuid` / `var_<n>.edge_uuid`).
        let in_schema = node.input.schema();
        let cols = node
            .targets
            .iter()
            .filter_map(|t: &DeleteTarget| {
                let qual = datafusion::common::TableReference::bare(format!("var_{}", t.var));
                let key = if t.is_edge { "edge_uuid" } else { "node_uuid" };
                let uuid_idx = in_schema.index_of_column_by_name(Some(&qual), key)?;
                Some(DeleteCol {
                    uuid_idx,
                    is_edge: t.is_edge,
                })
            })
            .collect();
        Self {
            input,
            cols,
            detach: node.detach,
            dir: node.dir.clone(),
            schema,
            props,
        }
    }
}

impl fmt::Debug for GraphDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphDeleteExec {{ targets: {}, detach: {} }}",
            self.cols.len(),
            self.detach
        )
    }
}

impl DisplayAs for GraphDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphDeleteExec: targets={}, detach={}",
            self.cols.len(),
            self.detach
        )
    }
}

impl ExecutionPlan for GraphDeleteExec {
    fn name(&self) -> &str {
        "GraphDeleteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("GraphDeleteExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            cols: self.cols.clone(),
            detach: self.detach,
            dir: self.dir.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        use futures::StreamExt;

        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraphDeleteExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let cols = self.cols.clone();
        let detach = self.detach;
        let dir = self.dir.clone();
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();

        let fut = async move {
            // Collect the targeted UUIDs from the matched rows, split by kind.
            let mut node_uuids: HashSet<[u8; 16]> = HashSet::new();
            let mut edge_uuids: HashSet<[u8; 16]> = HashSet::new();
            let mut stream = datafusion::physical_plan::execute_stream(input, context)?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                collect_delete_targets(&batch, &cols, &mut node_uuids, &mut edge_uuids)
                    .map_err(to_df_err)?;
            }

            // openCypher: a node may be deleted without DETACH only if every
            // relationship still incident to it is ALSO deleted by the same
            // statement. So `MATCH (a)-[r]->(b) DELETE r, a` is legal (r is gone
            // too), but deleting `a` while any *untargeted* edge remains on it is
            // an error. With DETACH, all incident edges are removed regardless.
            let incident =
                graphforge_storage::incident_edge_uuids(&dir, &node_uuids).map_err(to_df_err)?;
            if detach {
                edge_uuids.extend(incident);
            } else {
                // Only edges NOT already being deleted in this statement count as
                // "still has relationships".
                let survives = incident.iter().any(|e| !edge_uuids.contains(e));
                if survives {
                    return Err(to_df_err(GfError::Execution(
                        "Cannot delete node, because it still has relationships. To delete \
                         this node, you must first delete its relationships, or use DETACH DELETE."
                            .into(),
                    )));
                }
            }

            // One staged batch spanning edges + nodes (#790): a failure while
            // building any replacement file leaves the prior state intact, and
            // the commit renames `topology/nodes.parquet` last.
            let (nodes_deleted, edges_deleted) =
                graphforge_storage::delete_nodes_and_edges(&dir, &node_uuids, &edge_uuids)
                    .map_err(to_df_err)?;
            delete_summary_batch(&out_schema, nodes_deleted, edges_deleted).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            futures::stream::once(fut),
        )))
    }
}

/// Collect one input batch's delete-target uuids into the node/edge sets —
/// the per-batch DELETE collection phase, shared by [`GraphDeleteExec`] and
/// the mixed-write statement driver (#792).
///
/// openCypher: DELETE of a NULL is a no-op. An unmatched OPTIONAL MATCH row
/// has a null identity column — skip it rather than letting
/// `fixed_binary_uuid` error.
fn collect_delete_targets(
    batch: &RecordBatch,
    cols: &[DeleteCol],
    node_uuids: &mut HashSet<[u8; 16]>,
    edge_uuids: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    for col in cols {
        let id_col = batch.column(col.uuid_idx);
        for row in 0..batch.num_rows() {
            if id_col.is_null(row) {
                continue;
            }
            let uuid =
                graphforge_core::uuid::to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
            if col.is_edge {
                edge_uuids.insert(uuid);
            } else {
                node_uuids.insert(uuid);
            }
        }
    }
    Ok(())
}

/// The one-row delete summary `{nodes_deleted, edges_deleted}`.
fn delete_summary_batch(
    schema: &SchemaRef,
    nodes: u64,
    edges: u64,
) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![nodes])),
            Arc::new(UInt64Array::from(vec![edges])),
        ],
    )
    .map_err(|e| GfError::Execution(e.to_string()))
}

// ---------------------------------------------------------------------------
// GraphSetExec / GraphRemoveExec — physical nodes for SET / REMOVE (#791)
// ---------------------------------------------------------------------------

/// A one-column `UInt64` write summary (`properties_set` / `properties_removed`).
fn count_summary_batch(schema: &SchemaRef, count: u64) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![count]))],
    )
    .map_err(|e| GfError::Execution(e.to_string()))
}

/// Resolve a write target's input-column locations: the identity UUID column and
/// (for a node) the `type_id` column, or (for an edge) the `rel_type_name`
/// column used to route the property file per row.
#[derive(Clone)]
struct WriteCol {
    /// Property name being written / removed.
    prop_name: String,
    /// Column index of `var_<n>.node_uuid` / `var_<n>.edge_uuid`.
    uuid_idx: usize,
    is_edge: bool,
    /// Node target: index of `var_<n>.type_id` (`UInt32`), for entity-stem
    /// resolution in Strict/Advisory. `None` for an edge or when absent.
    type_id_idx: Option<usize>,
    /// Edge target: index of `var_<n>.rel_type_name` (`Utf8`) — the file stem.
    rel_name_idx: Option<usize>,
}

impl WriteCol {
    /// Resolve a target's columns from `in_schema` (the logical input schema,
    /// with `var_<n>` qualifiers), given its var, kind, and property name.
    /// Returns `None` if the identity column is absent (the var was not bound) —
    /// the caller treats that as a skip.
    fn resolve(in_schema: &DFSchema, var: u32, is_edge: bool, prop_name: &str) -> Option<Self> {
        let qual = datafusion::common::TableReference::bare(format!("var_{var}"));
        let key = if is_edge { "edge_uuid" } else { "node_uuid" };
        let uuid_idx = in_schema.index_of_column_by_name(Some(&qual), key)?;
        let (type_id_idx, rel_name_idx) = if is_edge {
            (
                None,
                in_schema.index_of_column_by_name(Some(&qual), "rel_type_name"),
            )
        } else {
            (
                in_schema.index_of_column_by_name(Some(&qual), "type_id"),
                None,
            )
        };
        Some(Self {
            prop_name: prop_name.to_owned(),
            uuid_idx,
            is_edge,
            type_id_idx,
            rel_name_idx,
        })
    }

    /// Compute the property-file stem for the entity in `batch` at `row`.
    ///
    /// Node: `_untyped` in Exploratory mode, else the entity name for the row's
    /// `type_id` (an unknown id falls back to `_untyped`). Edge: the row's
    /// `rel_type_name` value (the lowerer guarantees this column exists for an
    /// edge target).
    fn stem_for_row(
        &self,
        batch: &RecordBatch,
        row: usize,
        mode: OntologyMode,
        type_id_to_entity_name: &HashMap<u32, String>,
    ) -> Result<String, GfError> {
        if self.is_edge {
            let idx = self.rel_name_idx.ok_or_else(|| {
                GfError::Execution("edge SET/REMOVE target has no rel_type_name column".into())
            })?;
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| GfError::Execution("rel_type_name is not a string column".into()))?;
            return Ok(arr.value(row).to_owned());
        }
        // Node target.
        if mode == OntologyMode::Exploratory {
            return Ok(UNTYPED_STEM.to_owned());
        }
        let Some(idx) = self.type_id_idx else {
            return Err(GfError::Execution(
                "node SET/REMOVE in a typed ontology requires a type_id column on the matched var"
                    .into(),
            ));
        };
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .ok_or_else(|| GfError::Execution("type_id is not a UInt32 column".into()))?;
        let type_id = arr.value(row);
        Ok(type_id_to_entity_name
            .get(&type_id)
            .cloned()
            .unwrap_or_else(|| UNTYPED_STEM.to_owned()))
    }
}

/// Accumulator for per-stem, per-uuid property writes drained from the matched
/// rows, applied once per stem after the input stream is exhausted.
///
/// Node and edge writes are kept in **separate** maps even though their stems
/// can collide (both default to `_untyped`): node properties live under
/// `properties/<stem>.parquet`, edge properties under
/// `edge_properties/<stem>.parquet`, so they must be applied through different
/// storage primitives.
#[derive(Default)]
pub(crate) struct SetAccumulator {
    /// stem → uuid → { prop → value } for node targets.
    nodes: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>>,
    /// stem → uuid → { prop → value } for edge targets.
    edges: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>>,
}

impl SetAccumulator {
    fn record(
        &mut self,
        is_edge: bool,
        stem: String,
        uuid: [u8; 16],
        prop: String,
        value: IrLiteral,
    ) {
        let map = if is_edge {
            &mut self.edges
        } else {
            &mut self.nodes
        };
        map.entry(stem)
            .or_default()
            .entry(uuid)
            .or_default()
            .insert(prop, value);
    }

    fn forget(&mut self, is_edge: bool, stem: &str, uuid: &[u8; 16], prop: &str) {
        let map = if is_edge {
            &mut self.edges
        } else {
            &mut self.nodes
        };
        if let Some(by_uuid) = map.get_mut(stem) {
            if let Some(props) = by_uuid.get_mut(uuid) {
                props.remove(prop);
                if props.is_empty() {
                    by_uuid.remove(uuid);
                }
            }
            if by_uuid.is_empty() {
                map.remove(stem);
            }
        }
    }

    /// Stage all accumulated node + edge sets into `staged` (committed by the
    /// caller, #792), returning the number of distinct entities written.
    fn stage_into(
        &self,
        staged: &mut graphforge_storage::RewriteBatch,
        dir: &Path,
    ) -> Result<u64, GfError> {
        let mut total = 0u64;
        for (stem, updates) in &self.nodes {
            total += graphforge_storage::stage_set_node_properties(staged, dir, stem, updates)?;
        }
        for (stem, updates) in &self.edges {
            total += graphforge_storage::stage_set_edge_properties(staged, dir, stem, updates)?;
        }
        Ok(total)
    }

    /// Apply all accumulated node + edge sets through the storage primitives
    /// as one staged batch (#790 — a failure leaves every stem untouched),
    /// returning the total number of distinct entities written.
    fn apply(&self, dir: &Path) -> Result<u64, GfError> {
        let mut staged = graphforge_storage::RewriteBatch::new();
        let total = self.stage_into(&mut staged, dir)?;
        staged.commit_at(dir)?;
        Ok(total)
    }

    /// Drop every accumulated write targeting a uuid in `deleted` (#792): the
    /// entity is gone by statement end, so its property writes are
    /// unobservable and must not resurrect file rows.
    fn scrub(&mut self, deleted: &HashSet<[u8; 16]>) {
        for map in [&mut self.nodes, &mut self.edges] {
            map.retain(|_, by_uuid| {
                by_uuid.retain(|uuid, _| !deleted.contains(uuid));
                !by_uuid.is_empty()
            });
        }
    }
}

/// REMOVE analogue of [`SetAccumulator`].
#[derive(Default)]
pub(crate) struct RemoveAccumulator {
    nodes: HashMap<String, HashMap<[u8; 16], HashSet<String>>>,
    edges: HashMap<String, HashMap<[u8; 16], HashSet<String>>>,
}

impl RemoveAccumulator {
    fn record(&mut self, is_edge: bool, stem: String, uuid: [u8; 16], prop: String) {
        let map = if is_edge {
            &mut self.edges
        } else {
            &mut self.nodes
        };
        map.entry(stem)
            .or_default()
            .entry(uuid)
            .or_default()
            .insert(prop);
    }

    fn forget(&mut self, is_edge: bool, stem: &str, uuid: &[u8; 16], prop: &str) {
        let map = if is_edge {
            &mut self.edges
        } else {
            &mut self.nodes
        };
        if let Some(by_uuid) = map.get_mut(stem) {
            if let Some(props) = by_uuid.get_mut(uuid) {
                props.remove(prop);
                if props.is_empty() {
                    by_uuid.remove(uuid);
                }
            }
            if by_uuid.is_empty() {
                map.remove(stem);
            }
        }
    }

    /// Stage all accumulated removals into `staged` (committed by the caller).
    fn stage_into(
        &self,
        staged: &mut graphforge_storage::RewriteBatch,
        dir: &Path,
    ) -> Result<u64, GfError> {
        let mut total = 0u64;
        for (stem, removals) in &self.nodes {
            total += graphforge_storage::stage_remove_node_properties(staged, dir, stem, removals)?;
        }
        for (stem, removals) in &self.edges {
            total += graphforge_storage::stage_remove_edge_properties(staged, dir, stem, removals)?;
        }
        Ok(total)
    }

    /// One staged batch across all stems, like [`SetAccumulator::apply`].
    fn apply(&self, dir: &Path) -> Result<u64, GfError> {
        let mut staged = graphforge_storage::RewriteBatch::new();
        let total = self.stage_into(&mut staged, dir)?;
        staged.commit_at(dir)?;
        Ok(total)
    }

    /// REMOVE analogue of [`SetAccumulator::scrub`].
    fn scrub(&mut self, deleted: &HashSet<[u8; 16]>) {
        for map in [&mut self.nodes, &mut self.edges] {
            map.retain(|_, by_uuid| {
                by_uuid.retain(|uuid, _| !deleted.contains(uuid));
                !by_uuid.is_empty()
            });
        }
    }
}

/// Evaluate each SET target's value expression over one input batch and
/// record the per-row writes into `acc` — the per-batch SET phase, shared by
/// [`GraphSetExec`] and the mixed-write statement driver (#792).
///
/// openCypher: SET on a NULL identity (an unmatched OPTIONAL row) is a no-op.
fn accumulate_set_batch(
    batch: &RecordBatch,
    targets: &[(WriteCol, DfExpr)],
    phys_values: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    mode: OntologyMode,
    type_map: &HashMap<u32, String>,
    acc: &mut SetAccumulator,
) -> Result<(), GfError> {
    let n = batch.num_rows();
    for ((col, _), phys) in targets.iter().zip(phys_values) {
        // Evaluate the value expr once for the whole batch → a column.
        let values = phys
            .evaluate(batch)
            .and_then(|cv| cv.into_array(n))
            .map_err(|e| GfError::Execution(e.to_string()))?;
        let id_col = batch.column(col.uuid_idx);
        for row in 0..n {
            if id_col.is_null(row) {
                continue;
            }
            let uuid =
                graphforge_core::uuid::to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
            let scalar = ScalarValue::try_from_array(&values, row)
                .map_err(|e| GfError::Execution(e.to_string()))?;
            let lit =
                scalar_to_ir_literal(&scalar).map_err(|e| GfError::Execution(e.to_string()))?;
            let stem = col.stem_for_row(batch, row, mode, type_map)?;
            acc.record(col.is_edge, stem, uuid, col.prop_name.clone(), lit);
        }
    }
    Ok(())
}

/// Record one input batch's property removals into `acc` — the per-batch
/// REMOVE phase, shared by [`GraphRemoveExec`] and the mixed-write statement
/// driver (#792). A NULL identity (unmatched OPTIONAL row) is a no-op.
fn accumulate_remove_batch(
    batch: &RecordBatch,
    targets: &[WriteCol],
    mode: OntologyMode,
    type_map: &HashMap<u32, String>,
    acc: &mut RemoveAccumulator,
) -> Result<(), GfError> {
    for col in targets {
        let id_col = batch.column(col.uuid_idx);
        for row in 0..batch.num_rows() {
            if id_col.is_null(row) {
                continue;
            }
            let uuid =
                graphforge_core::uuid::to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
            let stem = col.stem_for_row(batch, row, mode, type_map)?;
            acc.record(col.is_edge, stem, uuid, col.prop_name.clone());
        }
    }
    Ok(())
}

/// Physical execution node for `SET <prop> = <expr>` (#791).
///
/// Drains its input (the preceding `MATCH`), evaluates each target's value
/// expression per row (the [`UnwindExec`] eval pattern), converts the result to
/// an [`IrLiteral`], and accumulates per-stem/per-uuid writes — then rewrites the
/// affected property files via the [`graphforge_storage`] SET primitives once. Emits a
/// one-row `properties_set` summary.
///
/// A NULL identity column (an unmatched `OPTIONAL MATCH` row) is a per-row no-op.
/// When a uuid appears in several matched rows, the last row's value wins.
pub struct GraphSetExec {
    input: Arc<dyn ExecutionPlan>,
    /// Per target: resolved columns + the value expression to evaluate per row.
    targets: Vec<(WriteCol, DfExpr)>,
    type_id_to_entity_name: HashMap<u32, String>,
    mode: OntologyMode,
    dir: PathBuf,
    /// Logical input schema (with `var_<n>` qualifiers) — used to build the
    /// per-target physical value exprs.
    in_df_schema: DFSchemaRef,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl GraphSetExec {
    /// Build the physical SET node from its logical counterpart and input.
    #[must_use]
    pub fn new(node: &GraphSetNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = GraphSetNode::summary_schema();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let in_df_schema = node.input.schema().clone();
        let targets = node
            .targets
            .iter()
            .filter_map(|t: &SetTarget| {
                let col = WriteCol::resolve(&in_df_schema, t.var, t.is_edge, &t.prop_name)?;
                Some((col, t.value.clone()))
            })
            .collect();
        Self {
            input,
            targets,
            type_id_to_entity_name: node.type_id_to_entity_name.clone(),
            mode: node.mode,
            dir: node.dir.clone(),
            in_df_schema,
            schema,
            props,
        }
    }
}

impl fmt::Debug for GraphSetExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphSetExec {{ targets: {} }}", self.targets.len())
    }
}

impl DisplayAs for GraphSetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphSetExec: targets={}", self.targets.len())
    }
}

impl ExecutionPlan for GraphSetExec {
    fn name(&self) -> &str {
        "GraphSetExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("GraphSetExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            targets: self.targets.clone(),
            type_id_to_entity_name: self.type_id_to_entity_name.clone(),
            mode: self.mode,
            dir: self.dir.clone(),
            in_df_schema: self.in_df_schema.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        use futures::StreamExt;

        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraphSetExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let targets = self.targets.clone();
        let type_map = self.type_id_to_entity_name.clone();
        let mode = self.mode;
        let dir = self.dir.clone();
        let df_schema = self.in_df_schema.clone();
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();

        let fut = async move {
            // Pre-build one physical value expr per target (UnwindExec pattern).
            let phys_values = targets
                .iter()
                .map(|(_, expr)| create_physical_expr(expr, &df_schema, &ExecutionProps::new()))
                .collect::<Result<Vec<_>, _>>()?;

            let mut acc = SetAccumulator::default();
            let mut stream = datafusion::physical_plan::execute_stream(input, context)?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                accumulate_set_batch(&batch, &targets, &phys_values, mode, &type_map, &mut acc)
                    .map_err(to_df_err)?;
            }

            let total = acc.apply(&dir).map_err(to_df_err)?;
            count_summary_batch(&out_schema, total).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            futures::stream::once(fut),
        )))
    }
}

/// Physical execution node for `REMOVE <prop>` (#791) — the value-less dual of
/// [`GraphSetExec`].
///
/// Drains its input, accumulates per-stem/per-uuid property removals, and
/// rewrites the affected property files via the [`graphforge_storage`] REMOVE primitives
/// once. Removing an absent property / uuid is a no-op (openCypher). Emits a
/// one-row `properties_removed` summary.
pub struct GraphRemoveExec {
    input: Arc<dyn ExecutionPlan>,
    targets: Vec<WriteCol>,
    type_id_to_entity_name: HashMap<u32, String>,
    mode: OntologyMode,
    dir: PathBuf,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl GraphRemoveExec {
    /// Build the physical REMOVE node from its logical counterpart and input.
    #[must_use]
    pub fn new(node: &GraphRemoveNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = GraphRemoveNode::summary_schema();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let in_df_schema = node.input.schema();
        let targets = node
            .targets
            .iter()
            .filter_map(|t: &RemoveTarget| {
                WriteCol::resolve(in_df_schema, t.var, t.is_edge, &t.prop_name)
            })
            .collect();
        Self {
            input,
            targets,
            type_id_to_entity_name: node.type_id_to_entity_name.clone(),
            mode: node.mode,
            dir: node.dir.clone(),
            schema,
            props,
        }
    }
}

impl fmt::Debug for GraphRemoveExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphRemoveExec {{ targets: {} }}", self.targets.len())
    }
}

impl DisplayAs for GraphRemoveExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphRemoveExec: targets={}", self.targets.len())
    }
}

impl ExecutionPlan for GraphRemoveExec {
    fn name(&self) -> &str {
        "GraphRemoveExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("GraphRemoveExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            targets: self.targets.clone(),
            type_id_to_entity_name: self.type_id_to_entity_name.clone(),
            mode: self.mode,
            dir: self.dir.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        use futures::StreamExt;

        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraphRemoveExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let targets = self.targets.clone();
        let type_map = self.type_id_to_entity_name.clone();
        let mode = self.mode;
        let dir = self.dir.clone();
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();

        let fut = async move {
            let mut acc = RemoveAccumulator::default();
            let mut stream = datafusion::physical_plan::execute_stream(input, context)?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                accumulate_remove_batch(&batch, &targets, mode, &type_map, &mut acc)
                    .map_err(to_df_err)?;
            }

            let total = acc.apply(&dir).map_err(to_df_err)?;
            count_summary_batch(&out_schema, total).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            futures::stream::once(fut),
        )))
    }
}

// ---------------------------------------------------------------------------
// VarLenExpandExec — physical node for variable-length Expand
// ---------------------------------------------------------------------------

/// Physical execution node for variable-length path expansion
/// (`(a)-[:R*min..max]->(b)`), the physical counterpart of
/// [`VarLenExpandNode`].
///
/// Performs an iterative BFS over the project's edge table (read directly via
/// [`graphforge_storage::read_edges`], since the DataFusion `TaskContext` exposes no
/// catalog).  Path uniqueness follows openCypher's **relationship isomorphism**:
/// no edge is traversed twice within a single path, which also makes unbounded
/// (`max_hops = None`) expansion terminate on cyclic graphs.
///
/// Output rows carry the input (source) columns followed by the reached
/// destination node's [`TOPOLOGY_NODES_SCHEMA`](graphforge_storage) columns.  The edge
/// variable is **not** bound (deferred — see [`VarLenExpandNode`]).
pub struct VarLenExpandExec {
    input: Arc<dyn ExecutionPlan>,
    rel_type_name: String,
    direction: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    dir: PathBuf,
    mode: OntologyMode,
    /// Column index of the BFS seed (`var_<src_var>.node_id`) within the input.
    ///
    /// Resolved from the *logical* input schema, whose qualifiers distinguish
    /// the source's `node_id` from any other `node_id` columns a prior
    /// expansion may have appended (arrow strips qualifiers, so a by-name
    /// lookup at execution time would ambiguously match the first `node_id`).
    src_col_idx: usize,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    /// Adjacency source for the BFS — the session-scoped provider injected
    /// by the extension planner (#761).
    provider: Arc<dyn AdjacencyProvider>,
}

impl VarLenExpandExec {
    /// Build the physical node from its logical counterpart, planned input,
    /// and the session's adjacency provider (#761).
    #[must_use]
    pub fn new(
        node: &VarLenExpandNode,
        input: Arc<dyn ExecutionPlan>,
        provider: Arc<dyn AdjacencyProvider>,
    ) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        // The seed is the source variable's node_id. Resolve its column index
        // from the qualified logical input schema (`var_<src>.node_id`); fall
        // back to 0 only if the binder shape is unexpected (the input always
        // leads with the source scan, so column 0 is the safe default).
        let src_qual = datafusion::common::TableReference::bare(format!("var_{}", node.src_var));
        let src_col_idx = node
            .input
            .schema()
            .index_of_column_by_name(Some(&src_qual), "node_id")
            .unwrap_or(0);
        Self {
            input,
            rel_type_name: node.rel_type_name.clone(),
            direction: node.direction,
            min_hops: node.min_hops,
            max_hops: node.max_hops,
            dir: node.dir.clone(),
            mode: node.mode,
            src_col_idx,
            schema,
            props,
            provider,
        }
    }
}

impl fmt::Debug for VarLenExpandExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VarLenExpandExec {{ rel: {}, hops: {}..{:?}, dir: {:?} }}",
            self.rel_type_name, self.min_hops, self.max_hops, self.direction
        )
    }
}

impl DisplayAs for VarLenExpandExec {
    /// Plan-display line, including how adjacency would be served
    /// (`adjacency=hit|miss|building`, #762). Until the persistent index
    /// lands (#761) the scan-build provider always reports `building`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let max = self.max_hops.map_or("*".to_owned(), |h| h.to_string());
        write!(
            f,
            "VarLenExpandExec: rel={}, hops={}..{}, adjacency={}",
            self.rel_type_name,
            self.min_hops,
            max,
            self.provider
                .status(&self.rel_type_name, self.direction)
                .as_str()
        )
    }
}

impl ExecutionPlan for VarLenExpandExec {
    fn name(&self) -> &str {
        "VarLenExpandExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("VarLenExpandExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            schema: self.schema.clone(),
            props: self.props.clone(),
            provider: self.provider.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "VarLenExpandExec only has partition 0, got {partition}"
            )));
        }
        // The BFS must consume the whole input frontier before emitting, so it
        // runs in a future that collects the child stream, then yields a single
        // output batch.
        let input = self.input.clone();
        let cfg = ExpandConfig {
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            out_schema: self.schema.clone(),
            provider: self.provider.clone(),
        };
        let schema = self.schema.clone();
        let fut = async move {
            let input_batches = collect(input, context).await?;
            expand_bfs(&cfg, &input_batches).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut),
        )))
    }
}

/// Pass-through physical node for [`OntologyInferNode`](graphforge_plan::OntologyInferNode)
/// (#605). VarLenExpand already computes the transitive/symmetric closure, so this
/// simply delegates to its single input. It exists so (a) the extension planner has
/// a handler (an unhandled extension node panics at plan time), and (b) the physical
/// plan + `explain()` surface the inference `rule_id`. The provenance event itself is
/// recorded once per rule by the execution session (see `record_inference_provenance`).
pub struct OntologyInferExec {
    input: Arc<dyn ExecutionPlan>,
    rule_id: String,
    confidence_model: String,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl OntologyInferExec {
    /// Build the pass-through from its logical counterpart + planned input.
    #[must_use]
    pub fn new(node: &graphforge_plan::OntologyInferNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            rule_id: node.rule_id.clone(),
            confidence_model: node.confidence_model.clone(),
            schema,
            props,
        }
    }
}

impl fmt::Debug for OntologyInferExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OntologyInferExec {{ rule_id: {} }}", self.rule_id)
    }
}

impl DisplayAs for OntologyInferExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OntologyInferExec: rule_id={}, confidence_model={}",
            self.rule_id, self.confidence_model
        )
    }
}

impl ExecutionPlan for OntologyInferExec {
    fn name(&self) -> &str {
        "OntologyInferExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("OntologyInferExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            rule_id: self.rule_id.clone(),
            confidence_model: self.confidence_model.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        // Pass-through: the wrapped input already produced the closure rows.
        self.input.execute(partition, context)
    }
}

/// Owned configuration for [`expand_bfs`] (so the BFS can run in a `'static`
/// future without borrowing the exec node).
struct ExpandConfig {
    rel_type_name: String,
    direction: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    dir: PathBuf,
    mode: OntologyMode,
    /// Column index of the BFS seed within the input batch (the source
    /// variable's `node_id`); resolved by qualifier at construction time.
    src_col_idx: usize,
    out_schema: SchemaRef,
    /// Adjacency source (#762) — moved into the `'static` execute future.
    provider: Arc<dyn AdjacencyProvider>,
}

/// One in-progress path during the variable-length BFS.
struct PathState {
    /// Current node (the path's frontier).
    node: u64,
    /// Edges already used on this path (relationship-isomorphism dedup). A
    /// `HashSet` for O(1) membership; `edge_path` keeps the ordered sequence.
    visited_edges: std::collections::HashSet<u64>,
    /// Edge ids in traversal order — the relationship list bound to the edge
    /// var (#709). `edge_path.len() == hops` by construction.
    edge_path: Vec<u64>,
    /// Hops taken so far.
    hops: u16,
    /// Index of the originating source row (to carry its input columns).
    input_row: usize,
}

/// Run the variable-length BFS and build the output batch.
///
/// Output columns = the input (source) columns, then the destination node's
/// `TOPOLOGY_NODES_SCHEMA` columns, then a trailing `List<Struct>` edge-list
/// column (the relationship list bound to the edge var, #709) — for every path
/// whose hop count lands in `[min_hops, max_hops]`.
fn expand_bfs(cfg: &ExpandConfig, input_batches: &[RecordBatch]) -> Result<RecordBatch, GfError> {
    use std::collections::HashMap;

    use arrow::compute::{concat_batches, take};

    let exec_err = |m: String| GfError::Execution(m);

    // --- Collect the source frontier (one combined batch). ---
    let input_schema = input_batches
        .first()
        .map_or_else(|| cfg.out_schema.clone(), RecordBatch::schema);
    let input =
        concat_batches(&input_schema, input_batches).map_err(|e| exec_err(e.to_string()))?;
    // Seed column is resolved by qualifier at construction (`var_<src>.node_id`),
    // so chained expansions — whose input carries several `node_id` columns —
    // start from the correct source rather than the first `node_id`.
    if cfg.src_col_idx >= input.num_columns() {
        return Err(exec_err(format!(
            "VarLenExpand source column index {} out of range ({} input columns)",
            cfg.src_col_idx,
            input.num_columns()
        )));
    }
    let src_ids = u64_column(&input, cfg.src_col_idx)?;

    // --- Obtain the directed adjacency the traversal needs (#762). ---
    let adjacency = cfg.provider.adjacency(&cfg.rel_type_name, cfg.direction)?;

    // --- BFS per source row, with per-path edge deduplication. ---
    // Run the traversal BEFORE any edge-file read: the BFS needs only the
    // adjacency view, and knowing the traversed edge ids lets the
    // relationship-list read below fetch exactly those rows (#830) instead of
    // scanning the whole file.
    let emissions = bfs_emit(cfg, &adjacency, src_ids);
    // No matched paths → empty output under the planned schema. Avoid assembling
    // take-columns from empty input/node batches: under DataFusion 54 an empty
    // seed can still produce wide intermediate schemas that disagree with
    // `out_schema` (columns vs fields mismatch).
    if emissions.is_empty() {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    }
    let traversed: std::collections::HashSet<u64> = emissions
        .iter()
        .flat_map(|(_, _, path)| path.iter().copied())
        .collect();

    // Edge records (public identity + rel type) keyed by edge_id, for assembling
    // the per-path relationship list (#709) — read lazily for the traversed
    // ids only (row-group pruning + row filter; an empty traversal never
    // opens the file).
    let edge_batches =
        graphforge_storage::read_edges_filtered(&cfg.dir, &cfg.rel_type_name, cfg.mode, &traversed)
            .map_err(|e| exec_err(e.to_string()))?;
    let edge_records = build_edge_records(cfg, &edge_batches)?;

    // --- Read nodes; map node_id -> row index for destination columns. ---
    // Lazily read only the reached destination node records (#838): the dst
    // columns are projected solely from these node_ids (incl. a 0-hop self
    // row's seed, which is in `emissions`), so an index Hit no longer scans the
    // whole node table. Source columns come from the input batch, not here.
    let reached: std::collections::HashSet<u64> = emissions.iter().map(|(_, id, _)| *id).collect();
    let node_batches = graphforge_storage::read_nodes_filtered(&cfg.dir, &reached)
        .map_err(|e| exec_err(e.to_string()))?;
    // `read_nodes_filtered` always returns at least one (possibly empty) batch,
    // but guard defensively: with no node batch there is nothing to reach, so
    // emit zero rows rather than indexing into an empty Vec.
    let Some(first) = node_batches.first() else {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    };
    let node_batch =
        concat_batches(&first.schema(), &node_batches).map_err(|e| exec_err(e.to_string()))?;
    let node_ids = u64_column(&node_batch, 1)?; // node_id is column 1
    let node_row: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .filter_map(|(i, id)| id.map(|v| (v, i)))
        .collect();

    // --- Materialise the output batch via `take` on input + node columns. ---
    let to_u32 = |v: usize| -> Result<u32, GfError> {
        u32::try_from(v).map_err(|_| exec_err(format!("row index {v} exceeds u32")))
    };
    let src_take = arrow::array::UInt32Array::from(
        emissions
            .iter()
            .map(|(r, _, _)| to_u32(*r))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let dst_take = arrow::array::UInt32Array::from(
        emissions
            .iter()
            .map(|(_, id, _)| {
                let row = node_row.get(id).ok_or_else(|| {
                    exec_err(format!(
                        "VarLenExpand reached unknown destination node_id {id}"
                    ))
                })?;
                to_u32(*row)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );

    let mut columns = Vec::with_capacity(input.num_columns() + node_batch.num_columns() + 1);
    for col in input.columns() {
        columns.push(take(col, &src_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    for col in node_batch.columns() {
        columns.push(take(col, &dst_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    // Trailing edge-list column (#709): one `List<Struct>` value per emitted
    // path, in schema order (last). Built from each path's ordered edge ids,
    // plus the relation's edge properties (#755).
    columns.push(build_edge_list_column(cfg, &emissions, &edge_records)?);
    RecordBatch::try_new(cfg.out_schema.clone(), columns).map_err(|e| exec_err(e.to_string()))
}

/// BFS every source seed over `adjacency`, with per-path relationship
/// isomorphism (no edge reused on a path). Returns one emission per matched
/// path: `(input_row, reached_node_id, edge_ids_in_order)`.
///
/// `min_hops == 0` (Cypher `*0..`) emits the 0-hop source-to-self path (empty
/// edge list); extension stops once `max_hops` is reached.
fn bfs_emit(
    cfg: &ExpandConfig,
    adjacency: &Adjacency,
    src_ids: &arrow::array::UInt64Array,
) -> Vec<(usize, u64, Vec<u64>)> {
    use std::collections::{HashSet, VecDeque};

    let mut emissions: Vec<(usize, u64, Vec<u64>)> = Vec::new();
    let mut queue: VecDeque<PathState> = VecDeque::new();
    for (row, seed) in src_ids.iter().enumerate() {
        if let Some(node) = seed {
            queue.push_back(PathState {
                node,
                visited_edges: HashSet::new(),
                edge_path: Vec::new(),
                hops: 0,
                input_row: row,
            });
        }
    }
    while let Some(p) = queue.pop_front() {
        let in_range = p.hops >= cfg.min_hops && cfg.max_hops.is_none_or(|m| p.hops <= m);
        if in_range {
            emissions.push((p.input_row, p.node, p.edge_path.clone()));
        }
        if cfg.max_hops.is_some_and(|m| p.hops >= m) {
            continue;
        }
        for (edge_id, next) in adjacency.neighbors(p.node).iter() {
            if p.visited_edges.contains(&edge_id) {
                continue; // relationship isomorphism: no edge twice per path
            }
            let mut visited = p.visited_edges.clone();
            visited.insert(edge_id);
            let mut edge_path = p.edge_path.clone();
            edge_path.push(edge_id);
            queue.push_back(PathState {
                node: next,
                visited_edges: visited,
                edge_path,
                hops: p.hops + 1,
                input_row: p.input_row,
            });
        }
    }
    emissions
}

/// The public identity of one edge, for the edge-list column (#709). UUIDs +
/// relation type only — never the surrogate `*_id` columns (UUID-only contract).
struct EdgeRecord {
    edge_uuid: [u8; 16],
    src_uuid: [u8; 16],
    dst_uuid: [u8; 16],
    rel_type: Option<String>,
}

/// Build `edge_id -> EdgeRecord` from the scanned edge batches, mirroring the
/// rel-type filtering the [`AdjacencyProvider`] applies when building the
/// traversal's adjacency view (#762).
///
/// Edge column layout (`graphforge-storage` schemas): edge_uuid=0, src_uuid=1,
/// dst_uuid=2 (FixedSizeBinary(16)); edge_id=3. The relation type comes from the
/// per-row `rel_type_name` column whenever the batch carries one (an exploratory
/// file, or the typed `"*"` union read — #823), else the config's concrete
/// `rel_type_name`.
fn build_edge_records(
    cfg: &ExpandConfig,
    edge_batches: &[RecordBatch],
) -> Result<std::collections::HashMap<u64, EdgeRecord>, GfError> {
    use std::collections::HashMap;

    let exec_err = |m: String| GfError::Execution(m);
    let fsb16 =
        |batch: &RecordBatch, idx: usize| -> Result<arrow::array::FixedSizeBinaryArray, GfError> {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .filter(|a| a.value_length() == 16)
                .cloned()
                .ok_or_else(|| {
                    exec_err(format!(
                        "expected FixedSizeBinary(16) edge column at index {idx}"
                    ))
                })
        };

    let mut records: HashMap<u64, EdgeRecord> = HashMap::new();
    for batch in edge_batches {
        let edge_ids = u64_column(batch, 3)?;
        let edge_uuids = fsb16(batch, 0)?;
        let src_uuids = fsb16(batch, 1)?;
        let dst_uuids = fsb16(batch, 2)?;
        // A batch carrying a `rel_type_name` column — an exploratory file, or
        // the typed `"*"` union read (#823) — supplies the per-edge relation and
        // is filtered to the requested relation (the `"*"` wildcard keeps every
        // row). A typed per-relation file has no such column, so gate on schema
        // presence, not ontology mode.
        let rel_names = batch
            .schema()
            .field_with_name("rel_type_name")
            .is_ok()
            .then(|| string_column(batch, "rel_type_name"))
            .transpose()?;
        let filter = rel_names.is_some() && cfg.rel_type_name != "*";

        for i in 0..batch.num_rows() {
            if let Some(names) = &rel_names
                && filter
                && names.value(i) != cfg.rel_type_name
            {
                continue;
            }
            let Some(edge_id) = edge_ids.value_at(i) else {
                continue;
            };
            // Per-row name in exploratory mode; else the config's concrete name.
            let rel_type = match &rel_names {
                Some(names) => Some(names.value(i).to_owned()),
                None => (cfg.rel_type_name != "*").then(|| cfg.rel_type_name.clone()),
            };
            let buf16 = |arr: &arrow::array::FixedSizeBinaryArray| -> [u8; 16] {
                let mut out = [0u8; 16];
                out.copy_from_slice(arr.value(i));
                out
            };
            records.entry(edge_id).or_insert(EdgeRecord {
                edge_uuid: buf16(&edge_uuids),
                src_uuid: buf16(&src_uuids),
                dst_uuid: buf16(&dst_uuids),
                rel_type,
            });
        }
    }
    Ok(records)
}

/// Assemble the trailing `List<Struct<{edge_uuid, src_uuid, dst_uuid, rel_type,
/// <props…>}>>` column (#709, #755): one sublist per emitted path, holding its
/// edges in traversal order.
///
/// The struct/field shape is derived from `cfg.out_schema` (the lowering-baked
/// schema — the single source of truth), so the produced column is byte-identical
/// to what the node advertises. The four topology fields are filled from
/// `edge_records`; any further fields are the relation's persisted edge
/// properties (#755), materialised by `take`ing each property column at the row
/// matching each hop's `edge_uuid` (null index → NULL for edges with no property
/// row, i.e. LEFT-join semantics).
fn build_edge_list_column(
    cfg: &ExpandConfig,
    emissions: &[(usize, u64, Vec<u64>)],
    edge_records: &std::collections::HashMap<u64, EdgeRecord>,
) -> Result<arrow::array::ArrayRef, GfError> {
    use arrow::array::{
        ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, StructArray, new_empty_array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::DataType;

    let exec_err = |m: String| GfError::Execution(m);

    // Derive the struct/list shape from the lowering-baked output schema (the
    // trailing column), the single source of truth — so the produced column is
    // byte-identical to the advertised schema with no second-source drift.
    let edge_field = cfg
        .out_schema
        .fields()
        .last()
        .ok_or_else(|| exec_err("output schema has no edge-list column".into()))?;
    let DataType::List(item) = edge_field.data_type() else {
        return Err(exec_err("edge-list field must be a List".into()));
    };
    let DataType::Struct(struct_fields) = item.data_type() else {
        return Err(exec_err("edge-list item must be a Struct".into()));
    };
    let struct_fields = struct_fields.clone();
    // Property fields are everything past the four topology fields, in order.
    let prop_fields: Vec<arrow::datatypes::FieldRef> =
        struct_fields.iter().skip(4).cloned().collect();

    // Flatten every hop of every path into the struct children, recording each
    // path's hop count for the list offsets. `edge_uuids` doubles as the lookup
    // key for the per-hop property take-index below.
    let mut edge_uuids: Vec<[u8; 16]> = Vec::new();
    let mut src_uuids: Vec<[u8; 16]> = Vec::new();
    let mut dst_uuids: Vec<[u8; 16]> = Vec::new();
    let mut rel_types: Vec<Option<String>> = Vec::new();
    let mut lengths: Vec<usize> = Vec::with_capacity(emissions.len());
    for (_, _, edge_path) in emissions {
        lengths.push(edge_path.len());
        for eid in edge_path {
            let rec = edge_records
                .get(eid)
                .ok_or_else(|| exec_err(format!("VarLenExpand: no record for edge_id {eid}")))?;
            edge_uuids.push(rec.edge_uuid);
            src_uuids.push(rec.src_uuid);
            dst_uuids.push(rec.dst_uuid);
            rel_types.push(rec.rel_type.clone());
        }
    }

    // FixedSizeBinaryArray::try_from_iter infers width from the first element;
    // when there are zero total hops it would yield a width-0 array that fails
    // the schema check. Build width-16 empty children explicitly in that case.
    let (edge_arr, src_arr, dst_arr): (ArrayRef, ArrayRef, ArrayRef) = if edge_uuids.is_empty() {
        (
            new_empty_array(&DataType::FixedSizeBinary(16)),
            new_empty_array(&DataType::FixedSizeBinary(16)),
            new_empty_array(&DataType::FixedSizeBinary(16)),
        )
    } else {
        let build = |v: Vec<[u8; 16]>| -> Result<ArrayRef, GfError> {
            Ok(Arc::new(
                FixedSizeBinaryArray::try_from_iter(v.into_iter())
                    .map_err(|e| exec_err(e.to_string()))?,
            ))
        };
        (
            build(edge_uuids.clone())?,
            build(src_uuids)?,
            build(dst_uuids)?,
        )
    };
    let rel_arr: ArrayRef = Arc::new(StringArray::from(rel_types));

    // Children in `struct_fields` order: edge_uuid, src_uuid, dst_uuid, rel_type,
    // then one array per property field (#755).
    let mut children: Vec<ArrayRef> = vec![edge_arr, src_arr, dst_arr, rel_arr];
    children.extend(build_edge_prop_children(
        &cfg.rel_type_name,
        &cfg.dir,
        &prop_fields,
        &edge_uuids,
    )?);

    let struct_arr =
        StructArray::try_new(struct_fields, children, None).map_err(|e| exec_err(e.to_string()))?;

    let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
    let list = ListArray::try_new(item.clone(), offsets, Arc::new(struct_arr), None)
        .map_err(|e| exec_err(e.to_string()))?;
    Ok(Arc::new(list))
}

/// Build one child array per edge-property struct field (#755), in field order.
///
/// Reads the relation's `edge_properties/<REL>.parquet` once (keyed by
/// `edge_uuid`), then `take`s each property column at the row matching each
/// flattened hop's `edge_uuid` — `None` for an edge with no property row yields a
/// NULL (LEFT-join semantics). A field advertised in the schema but absent on disk
/// becomes an all-NULL column. Returns an empty Vec when there are no prop fields.
/// `hop_edge_uuids` is in flattened hop order (matching the topology children).
///
/// A wildcard traversal (`*`, #1023) reads EVERY relation's property file:
/// each hop's values come from the file owning its `edge_uuid` (an edge belongs
/// to exactly one relation), coalesced per field — NULL where the owning
/// relation lacks the column, matching the lowering's nullable union schema.
fn build_edge_prop_children(
    rel_type_name: &str,
    dir: &Path,
    prop_fields: &[arrow::datatypes::FieldRef],
    hop_edge_uuids: &[[u8; 16]],
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    use std::collections::HashMap;

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, UInt32Array, new_null_array};
    use arrow::compute::kernels::zip::zip;
    use arrow::compute::{concat_batches, is_not_null, take};

    let exec_err = |m: String| GfError::Execution(m);
    if prop_fields.is_empty() {
        return Ok(Vec::new());
    }

    // Read each property file once and concat to a single batch per relation
    // (absent files contribute nothing). Typed traversals read one file; a
    // wildcard reads all of them, in the lowering's sorted-stem order.
    let stems: Vec<String> = if rel_type_name == "*" {
        graphforge_storage::list_edge_property_stems(dir)
    } else {
        vec![rel_type_name.to_owned()]
    };
    let mut prop_batches_by_rel = Vec::with_capacity(stems.len());
    let property_names = prop_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    for stem in &stems {
        let batches =
            graphforge_storage::read_edge_properties_projected(dir, stem, &property_names)
                .map_err(|e| exec_err(e.to_string()))?;
        if let Some(first) = batches.first() {
            prop_batches_by_rel.push(
                concat_batches(&first.schema(), &batches).map_err(|e| exec_err(e.to_string()))?,
            );
        }
    }

    // edge_uuid -> (owning batch, row within it). An edge belongs to exactly
    // one relation, so first-wins is a no-op for well-formed data.
    let mut uuid_to_loc: HashMap<[u8; 16], (usize, u32)> = HashMap::new();
    for (bi, b) in prop_batches_by_rel.iter().enumerate() {
        let key = b
            .column_by_name("edge_uuid")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
            // `downcast_ref` accepts any fixed-width binary column; require
            // width 16 so `copy_from_slice` into `[u8; 16]` can't panic on a
            // malformed on-disk file.
            .filter(|a| a.value_length() == 16)
            .ok_or_else(|| {
                exec_err("edge-property file missing a FixedSizeBinary(16) edge_uuid column".into())
            })?;
        for r in 0..key.len() {
            // A null key would `copy_from_slice` 16 zero bytes (a bogus UUID);
            // surface a corrupt file as an error instead.
            if key.is_null(r) {
                return Err(exec_err(format!(
                    "edge-property file has a null edge_uuid at row {r}"
                )));
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(key.value(r));
            uuid_to_loc.entry(u).or_insert((
                bi,
                u32::try_from(r)
                    .map_err(|_| exec_err(format!("edge-property row {r} exceeds u32")))?,
            ));
        }
    }

    // One take-index per flattened hop PER BATCH: Some(row) only for the hops
    // the batch owns, so each batch's `take` yields NULL everywhere else and
    // the per-field coalesce below can simply prefer non-null.
    let take_by_batch: Vec<UInt32Array> = (0..prop_batches_by_rel.len())
        .map(|bi| {
            hop_edge_uuids
                .iter()
                .map(|u| match uuid_to_loc.get(u) {
                    Some(&(owner, row)) if owner == bi => Some(row),
                    _ => None,
                })
                .collect()
        })
        .collect();

    let mut children: Vec<ArrayRef> = Vec::with_capacity(prop_fields.len());
    for field in prop_fields {
        let mut child: ArrayRef = new_null_array(field.data_type(), hop_edge_uuids.len());
        for (bi, b) in prop_batches_by_rel.iter().enumerate() {
            // Field advertised in the union schema but absent in this file
            // (or absent on disk entirely) -> this relation contributes NULLs.
            let Some(col) = b.column_by_name(field.name()) else {
                continue;
            };
            let taken = take(col, &take_by_batch[bi], None).map_err(|e| exec_err(e.to_string()))?;
            child = if prop_batches_by_rel.len() == 1 {
                // Single relation: `take` alone reproduces the #755 behavior.
                taken
            } else {
                let mask = is_not_null(&taken).map_err(|e| exec_err(e.to_string()))?;
                zip(&mask, &taken, &child).map_err(|e| exec_err(e.to_string()))?
            };
        }
        children.push(child);
    }
    Ok(children)
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
// ExpandExec — adjacency-backed single-hop expansion (#763)
// ---------------------------------------------------------------------------

/// Generation-pinned destination-identity resolver shared by every query and
/// hop of one facade. Replacement is atomic with facade generation adoption;
/// execution never rediscovers mutable identity files by path.
#[derive(Debug, Default)]
pub struct V4OrdinalIdentityResolver {
    handle: RwLock<
        Option<Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>>,
    >,
}

impl V4OrdinalIdentityResolver {
    /// Construct a resolver for an optional admitted generation facet.
    #[must_use]
    pub fn new(
        handle: Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>,
    ) -> Self {
        Self {
            handle: RwLock::new(handle.map(|handle| Arc::new(Mutex::new(handle)))),
        }
    }

    /// Replace the exact generation served by subsequent sessions.
    pub fn replace(
        &self,
        handle: Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>,
    ) {
        *self.handle.write().expect("ordinal identity lock poisoned") =
            handle.map(|handle| Arc::new(Mutex::new(handle)));
    }

    fn pin(&self) -> Result<Option<Arc<V4OrdinalIdentitySession>>, GfError> {
        let handle = self
            .handle
            .read()
            .expect("ordinal identity lock poisoned")
            .clone();
        let Some(handle) = handle else {
            return Ok(None);
        };
        let revalidation = handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .revalidate_for_session()
            .map_err(|error| GfError::Execution(error.to_string()))?;
        Ok(Some(Arc::new(V4OrdinalIdentitySession {
            handle,
            revalidation,
            attribution_available: AtomicBool::new(true),
        })))
    }
}

/// One exact, already-authenticated ordinal authority pinned for the lifetime
/// of an execution session.
#[derive(Debug)]
struct V4OrdinalIdentitySession {
    handle: Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>,
    revalidation: graphforge_storage::V4OrdinalRevalidationMetrics,
    attribution_available: AtomicBool,
}

impl V4OrdinalIdentitySession {
    fn lookup_node_uuids(
        &self,
        requested: &[u64],
    ) -> Result<graphforge_storage::V4OrdinalLookup, GfError> {
        let mut lookup = self
            .handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .lookup_node_uuids_pinned(requested)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        if self.attribution_available.swap(false, Ordering::AcqRel) {
            lookup.metrics.revalidation_calls = self.revalidation.calls;
            lookup.metrics.revalidation_bytes = self.revalidation.bytes_read;
        }
        Ok(lookup)
    }
}

/// Physical node for adjacency-backed single-hop expansion, the physical
/// counterpart of [`graphforge_plan::ExpandNode`].
///
/// Probes the session's [`AdjacencyProvider`] per frontier row instead of
/// hash-joining the full edge table; emits exactly the rows (and column
/// layout) the join chain would have produced. For `Undirected` the lowerer
/// wraps the node in `DISTINCT` (mirroring the join path's union+distinct),
/// so this node emits the provider's merged view raw — including a
/// self-loop's two entries, which the `DISTINCT` collapses.
pub struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    rel_type_name: String,
    direction: Direction,
    dir: PathBuf,
    mode: OntologyMode,
    /// Column index of the source variable's `node_id` in the input
    /// (qualifier-resolved at construction, like `VarLenExpandExec`).
    src_col_idx: usize,
    /// How many trailing `var_<edge>` schema fields are edge-property columns.
    edge_prop_count: usize,
    /// Number of columns contributed by the input (schema prefix length).
    input_width: usize,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    provider: Arc<dyn AdjacencyProvider>,
    /// Maximum rows this operator should emit after physical limit pushdown.
    fetch: Option<usize>,
    /// Edge binding id used as a stable diagnostic hop key.
    edge_var: u32,
    /// Initial resumable output-batch goal propagated through selective filters.
    demand_batch: Option<usize>,
    /// Query-scoped terminal cancellation shared by the bounded hop chain.
    demand: Option<Arc<demand::QueryDemand>>,
    /// Exact output columns consumed above this operator. `None` preserves the
    /// standalone full-schema contract.
    required_output: Option<Arc<[bool]>>,
    /// Facade-owned generation-pinned ordinal identity authority.
    ordinal_identities: Option<Arc<V4OrdinalIdentitySession>>,
    /// A facade configured an identity authority, but admission may have
    /// failed closed for this generation. Standalone sessions leave this false.
    ordinal_identity_required: bool,
}

impl ExpandExec {
    /// Build the physical node from its logical counterpart, planned input,
    /// and the session's adjacency provider.
    #[must_use]
    pub(crate) fn new(
        node: &graphforge_plan::ExpandNode,
        input: Arc<dyn ExecutionPlan>,
        provider: Arc<dyn AdjacencyProvider>,
        ordinal_identities: Option<Arc<V4OrdinalIdentitySession>>,
        ordinal_identity_required: bool,
    ) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let src_qual = datafusion::common::TableReference::bare(format!("var_{}", node.src_var));
        let src_col_idx = node
            .input
            .schema()
            .index_of_column_by_name(Some(&src_qual), "node_id")
            .unwrap_or(0);
        Self {
            input,
            rel_type_name: node.rel_type_name.clone(),
            direction: node.direction,
            dir: node.dir.clone(),
            mode: node.mode,
            src_col_idx,
            edge_prop_count: node.edge_prop_count,
            input_width: node.input.schema().fields().len(),
            schema,
            props,
            provider,
            fetch: None,
            edge_var: node.edge_var,
            demand_batch: None,
            demand: None,
            required_output: None,
            ordinal_identities,
            ordinal_identity_required,
        }
    }

    fn with_demand(
        &self,
        batch_goal: usize,
        demand: Arc<demand::QueryDemand>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            fetch: self.fetch,
            edge_var: self.edge_var,
            demand_batch: Some(batch_goal),
            demand: Some(demand),
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        })
    }

    fn with_required_output(&self, required: Vec<bool>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            fetch: self.fetch,
            edge_var: self.edge_var,
            demand_batch: self.demand_batch,
            demand: self.demand.clone(),
            required_output: Some(required.into()),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        })
    }
}

impl fmt::Debug for ExpandExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExpandExec {{ rel: {}, dir: {:?}, fetch: {:?}, demand_batch: {:?} }}",
            self.rel_type_name, self.direction, self.fetch, self.demand_batch
        )
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let arrow = match self.direction {
            Direction::Out => "->",
            Direction::In => "<-",
            Direction::Undirected => "--",
        };
        write!(
            f,
            "ExpandExec: rel={}, dir={arrow}, adjacency={}, fetch={}, demand_batch={}, projection={}, cancel={}",
            self.rel_type_name,
            self.provider
                .status(&self.rel_type_name, self.direction)
                .as_str(),
            self.fetch
                .map_or_else(|| "all".to_owned(), |n| n.to_string()),
            self.demand_batch
                .map_or_else(|| "all".to_owned(), |n| n.to_string()),
            self.required_output.as_ref().map_or_else(
                || "all".to_owned(),
                |mask| mask.iter().filter(|needed| **needed).count().to_string(),
            ),
            if self.demand.is_some() {
                "guarded"
            } else {
                "none"
            }
        )
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("ExpandExec needs one child".into()))?;
        Ok(Arc::new(Self {
            input,
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            schema: self.schema.clone(),
            props: self.props.clone(),
            provider: self.provider.clone(),
            fetch: self.fetch,
            edge_var: self.edge_var,
            demand_batch: self.demand_batch,
            demand: self.demand.clone(),
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        }))
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            fetch,
            edge_var: self.edge_var,
            demand_batch: self.demand_batch,
            demand: self.demand.clone(),
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // ExpandExec has one output partition and explicitly coalesces a
        // multi-partition child with execute_stream. Advertising a benefit here
        // inserts an eager round-robin exchange that cannot parallelize the hop.
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ExpandExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let cfg = SingleHopConfig {
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            out_schema: self.schema.clone(),
            provider: self.provider.clone(),
            edge_var: self.edge_var,
            demand: self.demand.clone(),
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        };
        let schema = self.schema.clone();
        let batch_size = context.session_config().batch_size();
        let input_stream = datafusion::physical_plan::execute_stream(input, context)?;
        let remaining = self.fetch;
        let initial_batch_goal = self
            .demand_batch
            .map_or(batch_size, |goal| goal.min(batch_size));
        let stream = futures::stream::try_unfold(
            (
                input_stream,
                cfg,
                remaining,
                None,
                batch_size,
                initial_batch_goal,
            ),
            |(
                mut input_stream,
                cfg,
                mut remaining,
                mut pending,
                batch_size,
                mut next_batch_goal,
            )| async move {
                loop {
                    if remaining == Some(0)
                        || cfg
                            .demand
                            .as_ref()
                            .is_some_and(|demand| demand.is_cancelled())
                    {
                        return Ok(None);
                    }
                    if let Some((input_batch, position)) = pending.as_mut() {
                        let max_output =
                            remaining.map_or(next_batch_goal, |left| left.min(next_batch_goal));
                        let output =
                            expand_single_hop_chunk(&cfg, input_batch, position, max_output)
                                .map_err(to_df_err)?;
                        if position.row >= input_batch.num_rows() {
                            pending = None;
                        }
                        if let Some(left) = remaining.as_mut() {
                            *left = left.saturating_sub(output.num_rows());
                        }
                        if output.num_rows() == 0 {
                            continue;
                        }
                        if remaining.is_none() {
                            next_batch_goal = next_batch_goal.saturating_mul(2).min(batch_size);
                        }
                        return Ok(Some((
                            output,
                            (
                                input_stream,
                                cfg,
                                remaining,
                                pending,
                                batch_size,
                                next_batch_goal,
                            ),
                        )));
                    }
                    let Some(input_batch) = input_stream.next().await else {
                        return Ok(None);
                    };
                    let input_batch = input_batch?;
                    demand::record_input(cfg.edge_var, input_batch.num_rows());
                    pending = Some((input_batch, SingleHopPosition::default()));
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Owned configuration for [`expand_single_hop_chunk`] (the BFS-free single-hop
/// analogue of [`ExpandConfig`]).
struct SingleHopConfig {
    rel_type_name: String,
    direction: Direction,
    dir: PathBuf,
    mode: OntologyMode,
    src_col_idx: usize,
    edge_prop_count: usize,
    input_width: usize,
    out_schema: SchemaRef,
    provider: Arc<dyn AdjacencyProvider>,
    edge_var: u32,
    demand: Option<Arc<demand::QueryDemand>>,
    required_output: Option<Arc<[bool]>>,
    ordinal_identities: Option<Arc<V4OrdinalIdentitySession>>,
    ordinal_identity_required: bool,
}

/// Resumable position within one input batch. Keeping the raw adjacency offset
/// and undirected per-row dedup set lets an upstream fixed hop yield bounded
/// batches without losing high-degree neighbors when a chunk boundary lands
/// inside one source row.
#[derive(Default)]
struct SingleHopPosition {
    row: usize,
    neighbor_offset: usize,
    seen_edges: std::collections::HashSet<u64>,
}

/// Build a schema-valid value column for an output that is provably unused by
/// every operator above this Expand. Nullable fields use Arrow nulls; required
/// physical fields receive inert values so the unchanged logical schema stays
/// valid without forcing their backing Parquet columns to be read.
fn unused_expand_column(field: &Field, rows: usize) -> Result<ArrayRef, GfError> {
    if field.is_nullable() {
        return Ok(new_null_array(field.data_type(), rows));
    }
    let column: ArrayRef = match field.data_type() {
        DataType::FixedSizeBinary(width) => Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                (0..rows).map(|_| vec![0_u8; usize::try_from(*width).unwrap_or(0)]),
            )
            .map_err(|error| GfError::Execution(error.to_string()))?,
        ),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![0_u64; rows])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![0_u32; rows])),
        DataType::Utf8 => Arc::new(StringArray::from(vec![""; rows])),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let values = TimestampMicrosecondArray::from(vec![0_i64; rows]);
            Arc::new(if let Some(timezone) = timezone {
                values.with_timezone(Arc::clone(timezone))
            } else {
                values
            })
        }
        DataType::List(item) if item.data_type() == &DataType::UInt32 => {
            let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(Arc::clone(item));
            for _ in 0..rows {
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        data_type => {
            return Err(GfError::Execution(format!(
                "Expand cannot synthesize unused non-nullable output '{}' with type {data_type}",
                field.name()
            )));
        }
    };
    Ok(column)
}

/// Execute the adjacency-backed single-hop expansion: for every input row's
/// source node, emit one output row per adjacency entry, assembling input,
/// edge-topology, edge-property (nullable), and destination-node columns in
/// the [`graphforge_plan::ExpandNode`] schema order — the same rows the join chain
/// produces.
#[allow(clippy::too_many_lines)]
fn expand_single_hop_chunk(
    cfg: &SingleHopConfig,
    input: &RecordBatch,
    position: &mut SingleHopPosition,
    max_output: usize,
) -> Result<RecordBatch, GfError> {
    use std::collections::HashMap;

    use arrow::compute::{concat_batches, take};

    let exec_err = |m: String| GfError::Execution(m);

    if input.num_rows() == 0 || max_output == 0 {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    }
    if cfg.src_col_idx >= input.num_columns() {
        return Err(exec_err(format!(
            "Expand source column index {} out of range ({} input columns)",
            cfg.src_col_idx,
            input.num_columns()
        )));
    }
    let src_ids = u64_column(input, cfg.src_col_idx)?;

    // The adjacency view: directional for Out/In, merged for Undirected
    // (dedup per input row happens in the emit pass below).
    let adjacency = cfg.provider.adjacency(&cfg.rel_type_name, cfg.direction)?;

    // Pass 1: walk the frontier collecting (input row, edge_id, neighbor)
    // triples and the distinct traversed edge ids, so the edge read below
    // fetches exactly those rows (#830) instead of scanning the whole file.
    let mut triples: Vec<(usize, u64, u64)> = Vec::new();
    let mut traversed: std::collections::HashSet<u64> = std::collections::HashSet::new();
    // Reached destination node ids, for the lazy node-record read (#838).
    let mut reached: std::collections::HashSet<u64> = std::collections::HashSet::new();
    while position.row < input.num_rows() && triples.len() < max_output {
        let row = position.row;
        let Some(src) = src_ids.value_at(row) else {
            position.row += 1;
            position.neighbor_offset = 0;
            position.seen_edges.clear();
            continue;
        };
        let neighbors = adjacency.neighbors(src);
        while position.neighbor_offset < neighbors.len() && triples.len() < max_output {
            let (edge_id, neighbor) = neighbors
                .get(position.neighbor_offset)
                .expect("neighbor_offset < len");
            position.neighbor_offset += 1;
            if matches!(cfg.direction, Direction::Undirected)
                && !position.seen_edges.insert(edge_id)
            {
                continue;
            }
            triples.push((row, edge_id, neighbor));
            traversed.insert(edge_id);
            reached.insert(neighbor);
        }
        if position.neighbor_offset >= neighbors.len() {
            position.row += 1;
            position.neighbor_offset = 0;
            position.seen_edges.clear();
        }
    }
    if triples.is_empty() {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    }
    demand::record_candidates(cfg.edge_var, triples.len());

    let dst_width = graphforge_storage::TOPOLOGY_NODES_SCHEMA.fields().len();
    let edge_end = cfg.out_schema.fields().len().saturating_sub(dst_width);
    let required = cfg.required_output.as_deref();
    let edge_unused = required.is_some_and(|mask| {
        mask.get(cfg.input_width..edge_end)
            .is_some_and(|fields| fields.iter().all(|needed| !needed))
    });
    let destination_uuid_index = edge_end;
    let destination_id_index = edge_end + 1;
    let destination_identity_only = required.is_some_and(|mask| {
        mask.iter()
            .enumerate()
            .skip(edge_end)
            .all(|(index, needed)| {
                !needed || index == destination_uuid_index || index == destination_id_index
            })
    });
    let uuid_required =
        required.is_some_and(|mask| mask.get(destination_uuid_index).copied().unwrap_or(false));
    if edge_unused
        && destination_identity_only
        && uuid_required
        && cfg.ordinal_identity_required
        && cfg.ordinal_identities.is_none()
    {
        return Err(GfError::Execution(
            "destination UUID projection requires admitted v4 ordinal identity".into(),
        ));
    }
    if edge_unused
        && destination_identity_only
        && let Some(ordinal_identities) = cfg.ordinal_identities.as_ref()
    {
        let mut requested = reached.iter().copied().collect::<Vec<_>>();
        requested.sort_unstable();
        let (resolved, identity_metrics) = if uuid_required {
            let lookup = ordinal_identities.lookup_node_uuids(&requested)?;
            (lookup.values, lookup.metrics)
        } else {
            (
                vec![None; requested.len()],
                graphforge_storage::V4OrdinalLookupMetrics::default(),
            )
        };
        let mut uuids = HashMap::with_capacity(requested.len());
        for (node_id, uuid) in requested.into_iter().zip(resolved) {
            if uuid_required {
                let uuid = uuid.ok_or_else(|| {
                    GfError::Execution(format!(
                        "Expand reached unknown destination node_id {node_id}"
                    ))
                })?;
                uuids.insert(node_id, *uuid.as_bytes());
            }
        }
        let src_take = arrow::array::UInt32Array::from(
            triples
                .iter()
                .map(|(row, _, _)| {
                    u32::try_from(*row).map_err(|_| exec_err("row index exceeds u32".into()))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let mut columns = Vec::with_capacity(cfg.out_schema.fields().len());
        for column in input.columns() {
            columns
                .push(take(column, &src_take, None).map_err(|error| exec_err(error.to_string()))?);
        }
        for field in cfg
            .out_schema
            .fields()
            .iter()
            .skip(cfg.input_width)
            .take(edge_end.saturating_sub(cfg.input_width))
        {
            columns.push(unused_expand_column(field, triples.len())?);
        }
        for (offset, field) in cfg.out_schema.fields().iter().skip(edge_end).enumerate() {
            let index = edge_end + offset;
            let column: ArrayRef = if required.is_some_and(|mask| mask[index])
                && index == destination_id_index
            {
                Arc::new(UInt64Array::from(
                    triples
                        .iter()
                        .map(|(_, _, neighbor)| *neighbor)
                        .collect::<Vec<_>>(),
                ))
            } else if required.is_some_and(|mask| mask[index]) && index == destination_uuid_index {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(triples.len(), 16);
                for (_, _, neighbor) in &triples {
                    builder
                        .append_value(uuids[neighbor])
                        .map_err(|error| exec_err(error.to_string()))?;
                }
                Arc::new(builder.finish())
            } else {
                unused_expand_column(field, triples.len())?
            };
            columns.push(column);
        }
        let output = RecordBatch::try_new(cfg.out_schema.clone(), columns)
            .map_err(|error| exec_err(error.to_string()))?;
        let projected_columns = required.map_or(cfg.out_schema.fields().len(), |mask| {
            mask.iter().filter(|needed| **needed).count()
        });
        demand::record_identity_projection(
            cfg.edge_var,
            output.num_rows(),
            projected_columns,
            &identity_metrics,
        );
        demand::record_emitted(cfg.edge_var, output.num_rows());
        return Ok(output);
    }

    // Edge rows keyed by edge_id, for the edge topology columns — read
    // lazily for the traversed ids only.
    let edge_permit = cfg
        .demand
        .as_ref()
        .and_then(|state| state.begin_read(cfg.edge_var));
    if cfg.demand.is_some() && edge_permit.is_none() {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    }
    let edge_observer = demand::capture_enabled().then(|| {
        Arc::new(demand::HopReadObserver::new(cfg.edge_var))
            as Arc<dyn graphforge_storage::io_stats::FilteredReadObserver>
    });
    let edge_topology_width = edge_end
        .saturating_sub(cfg.input_width)
        .saturating_sub(cfg.edge_prop_count);
    let relationship_properties_required = required.is_none_or(|mask| {
        mask[cfg.input_width + edge_topology_width..edge_end]
            .iter()
            .any(|needed| *needed)
    });
    let mut edge_projection = (0..edge_topology_width)
        .filter(|offset| required.is_none_or(|mask| mask[cfg.input_width + offset]))
        .collect::<Vec<_>>();
    // edge_id keys adjacency entries; edge_uuid keys demanded relationship
    // properties. Storage adds edge_id automatically.
    if relationship_properties_required {
        edge_projection.push(0);
    }
    let edge_batches = if required.is_some() {
        graphforge_storage::read_edges_filtered_projected_observed(
            &cfg.dir,
            &cfg.rel_type_name,
            cfg.mode,
            &traversed,
            &edge_projection,
            edge_observer.as_ref(),
        )
    } else {
        graphforge_storage::read_edges_filtered_observed(
            &cfg.dir,
            &cfg.rel_type_name,
            cfg.mode,
            &traversed,
            edge_observer.as_ref(),
        )
    }
    .map_err(|e| exec_err(e.to_string()))?;
    drop(edge_permit);
    let edge_schema = edge_batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| exec_err("Expand: edge scan returned no batches".into()))?;
    let edge_batch =
        concat_batches(&edge_schema, &edge_batches).map_err(|e| exec_err(e.to_string()))?;
    let edge_id_index = edge_batch
        .schema()
        .index_of("edge_id")
        .map_err(|error| exec_err(error.to_string()))?;
    let edge_ids_col = u64_column(&edge_batch, edge_id_index)?;
    let edge_row: HashMap<u64, usize> = (0..edge_batch.num_rows())
        .filter_map(|i| edge_ids_col.value_at(i).map(|id| (id, i)))
        .collect();
    let edge_uuids = relationship_properties_required
        .then(|| {
            edge_batch
                .column_by_name("edge_uuid")
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .filter(|array| array.value_length() == 16)
                .ok_or_else(|| {
                    exec_err("Expand: edge_uuid column is not FixedSizeBinary(16)".into())
                })
        })
        .transpose()?;

    // Destination node rows keyed by node_id — read lazily for the reached
    // neighbors only (#838), so an index Hit does not scan the whole node table.
    let node_permit = cfg
        .demand
        .as_ref()
        .and_then(|state| state.begin_read(cfg.edge_var));
    if cfg.demand.is_some() && node_permit.is_none() {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    }
    let node_observer = demand::capture_enabled().then(|| {
        Arc::new(demand::HopReadObserver::new(cfg.edge_var))
            as Arc<dyn graphforge_storage::io_stats::FilteredReadObserver>
    });
    let node_projection = (0..dst_width)
        .filter(|offset| required.is_none_or(|mask| mask[edge_end + offset]))
        .collect::<Vec<_>>();
    let edge_key_index =
        if matches!(cfg.mode, OntologyMode::Exploratory) || cfg.rel_type_name == "*" {
            graphforge_storage::EXPLORATORY_EDGE_SCHEMA.index_of("edge_id")
        } else {
            graphforge_storage::TYPED_EDGE_SCHEMA.index_of("edge_id")
        }
        .map_err(|error| exec_err(error.to_string()))?;
    let edge_key_already_demanded = usize::from(edge_projection.contains(&edge_key_index));
    let node_key_already_demanded = usize::from(node_projection.contains(&1));
    demand::record_materialization_projection(
        cfg.edge_var,
        edge_projection
            .len()
            .saturating_add(1_usize.saturating_sub(edge_key_already_demanded))
            .saturating_add(required.map_or(cfg.edge_prop_count, |mask| {
                mask[cfg.input_width + edge_topology_width..edge_end]
                    .iter()
                    .filter(|needed| **needed)
                    .count()
            })),
        node_projection
            .len()
            .saturating_add(1_usize.saturating_sub(node_key_already_demanded)),
    );
    let node_batches = if required.is_some() {
        graphforge_storage::read_nodes_filtered_projected_observed(
            &cfg.dir,
            &reached,
            &node_projection,
            node_observer.as_ref(),
        )
    } else {
        graphforge_storage::read_nodes_filtered_observed(&cfg.dir, &reached, node_observer.as_ref())
    }
    .map_err(|e| exec_err(e.to_string()))?;
    drop(node_permit);
    let Some(first) = node_batches.first() else {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    };
    let node_batch =
        concat_batches(&first.schema(), &node_batches).map_err(|e| exec_err(e.to_string()))?;
    let node_id_index = node_batch
        .schema()
        .index_of("node_id")
        .map_err(|error| exec_err(error.to_string()))?;
    let node_ids = u64_column(&node_batch, node_id_index)?;
    let node_row: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .filter_map(|(i, id)| id.map(|v| (v, i)))
        .collect();

    // Pass 2: convert triples into take indices against the FILTERED edge
    // batch (edge_row maps edge_id -> position within it).
    let to_u32 = |v: usize| -> Result<u32, GfError> {
        u32::try_from(v).map_err(|_| exec_err(format!("row index {v} exceeds u32")))
    };
    let mut src_take: Vec<u32> = Vec::new();
    let mut edge_take: Vec<u32> = Vec::new();
    let mut dst_take: Vec<u32> = Vec::new();
    let mut output_edge_uuids: Vec<[u8; 16]> = Vec::new();
    for &(row, edge_id, neighbor) in &triples {
        let Some(&edge_idx) = edge_row.get(&edge_id) else {
            return Err(exec_err(format!(
                "Expand: adjacency entry references unknown edge_id {edge_id}"
            )));
        };
        let Some(&dst_idx) = node_row.get(&neighbor) else {
            return Err(exec_err(format!(
                "Expand reached unknown destination node_id {neighbor}"
            )));
        };
        src_take.push(to_u32(row)?);
        edge_take.push(to_u32(edge_idx)?);
        dst_take.push(to_u32(dst_idx)?);
        if let Some(edge_uuids) = edge_uuids {
            if edge_uuids.is_null(edge_idx) {
                return Err(exec_err(format!(
                    "Expand: edge_id {edge_id} has a null edge_uuid"
                )));
            }
            let mut edge_uuid = [0u8; 16];
            edge_uuid.copy_from_slice(edge_uuids.value(edge_idx));
            output_edge_uuids.push(edge_uuid);
        }
    }
    let src_take = arrow::array::UInt32Array::from(src_take);
    let edge_take = arrow::array::UInt32Array::from(edge_take);
    let dst_take = arrow::array::UInt32Array::from(dst_take);

    // Assemble columns in ExpandNode schema order: input ++ edge topology ++
    // edge properties (nullable) ++ destination node.
    let prop_fields: Vec<arrow::datatypes::FieldRef> = cfg
        .out_schema
        .fields()
        .iter()
        .skip(cfg.input_width + edge_topology_width)
        .take(cfg.edge_prop_count)
        .cloned()
        .collect();
    let mut columns = Vec::with_capacity(cfg.out_schema.fields().len());
    for column in input.columns() {
        columns.push(take(column, &src_take, None).map_err(|error| exec_err(error.to_string()))?);
    }
    for (offset, field) in cfg
        .out_schema
        .fields()
        .iter()
        .skip(cfg.input_width)
        .take(edge_topology_width)
        .enumerate()
    {
        let index = cfg.input_width + offset;
        columns.push(if required.is_none_or(|mask| mask[index]) {
            let column = edge_batch.column_by_name(field.name()).ok_or_else(|| {
                exec_err(format!(
                    "Expand: projected edge column {} is absent",
                    field.name()
                ))
            })?;
            take(column, &edge_take, None).map_err(|error| exec_err(error.to_string()))?
        } else {
            unused_expand_column(field, triples.len())?
        });
    }
    let demanded_property_fields = prop_fields
        .iter()
        .enumerate()
        .filter(|(offset, _)| {
            required.is_none_or(|mask| mask[cfg.input_width + edge_topology_width + offset])
        })
        .map(|(_, field)| Arc::clone(field))
        .collect::<Vec<_>>();
    let demanded_property_columns = build_edge_prop_children(
        &cfg.rel_type_name,
        &cfg.dir,
        &demanded_property_fields,
        &output_edge_uuids,
    )?;
    let demanded_properties = demanded_property_fields
        .iter()
        .map(|field| field.name().clone())
        .zip(demanded_property_columns)
        .collect::<HashMap<_, _>>();
    for field in &prop_fields {
        columns.push(
            demanded_properties
                .get(field.name())
                .cloned()
                .map_or_else(|| unused_expand_column(field, triples.len()), Ok)?,
        );
    }
    for (offset, field) in cfg.out_schema.fields().iter().skip(edge_end).enumerate() {
        let index = edge_end + offset;
        columns.push(if required.is_none_or(|mask| mask[index]) {
            let column = node_batch.column_by_name(field.name()).ok_or_else(|| {
                exec_err(format!(
                    "Expand: projected node column {} is absent",
                    field.name()
                ))
            })?;
            take(column, &dst_take, None).map_err(|error| exec_err(error.to_string()))?
        } else {
            unused_expand_column(field, triples.len())?
        });
    }
    let output = RecordBatch::try_new(cfg.out_schema.clone(), columns)
        .map_err(|e| exec_err(e.to_string()))?;
    demand::record_emitted(cfg.edge_var, output.num_rows());
    Ok(output)
}

// ---------------------------------------------------------------------------
// OptionalMatchExec — physical node for OPTIONAL MATCH
// ---------------------------------------------------------------------------

/// Physical execution node for `OPTIONAL MATCH`, the physical counterpart of
/// [`OptionalMatchNode`].
///
/// Left-joins the `outer` (mandatory) input against the `optional` sub-plan on
/// the shared-variable [`join_keys`], preserving every outer row and setting the
/// optional-side columns to **null** when there is no match (openCypher
/// null-shaping). The shared join-key columns are carried by the outer side and
/// dropped from the inner side of the output (consistent with
/// [`OptionalMatchNode`]'s schema). When `join_keys` is empty the match is
/// unconditional (every outer row pairs with every inner row, or null-shapes if
/// the inner side is empty).
pub struct OptionalMatchExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    join_keys: Vec<(usize, usize)>,
    /// Inner column indices to append to the output, in order (every shared-
    /// variable column already excluded — those come from the outer side).
    inner_keep_idx: Vec<usize>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl OptionalMatchExec {
    /// Build the physical node from its logical counterpart and planned inputs.
    #[must_use]
    pub fn new(
        node: &OptionalMatchNode,
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            outer,
            inner,
            join_keys: node.join_keys.clone(),
            inner_keep_idx: node.inner_keep_idx.clone(),
            schema,
            props,
        }
    }
}

impl fmt::Debug for OptionalMatchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OptionalMatchExec {{ join_keys: {} }}",
            self.join_keys.len()
        )
    }
}

impl DisplayAs for OptionalMatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OptionalMatchExec: keys={}", self.join_keys.len())
    }
}

impl ExecutionPlan for OptionalMatchExec {
    fn name(&self) -> &str {
        "OptionalMatchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.outer, &self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut it = children.into_iter();
        let (Some(outer), Some(inner)) = (it.next(), it.next()) else {
            return Err(DataFusionError::Internal(
                "OptionalMatchExec needs two children".into(),
            ));
        };
        Ok(Arc::new(Self {
            outer,
            inner,
            join_keys: self.join_keys.clone(),
            inner_keep_idx: self.inner_keep_idx.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OptionalMatchExec only has partition 0, got {partition}"
            )));
        }
        let outer = self.outer.clone();
        let inner = self.inner.clone();
        let cfg = OptionalConfig {
            join_keys: self.join_keys.clone(),
            inner_keep_idx: self.inner_keep_idx.clone(),
            out_schema: self.schema.clone(),
            // Carry the child schemas so concat_batches has a schema even when a
            // child yields zero batches (an empty inner must null-shape, not
            // error; an empty outer must use the outer — not full — schema).
            outer_schema: self.outer.schema(),
            inner_schema: self.inner.schema(),
        };
        let schema = self.schema.clone();
        let fut = async move {
            let outer_batches = collect(outer, context.clone()).await?;
            let inner_batches = collect(inner, context).await?;
            optional_join(&cfg, &outer_batches, &inner_batches).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut),
        )))
    }
}

/// Owned config for [`optional_join`] (so it can run in a `'static` future).
struct OptionalConfig {
    join_keys: Vec<(usize, usize)>,
    /// Inner column indices to append (in order); every shared-variable column
    /// already excluded. Source of truth shared with the node's output schema.
    inner_keep_idx: Vec<usize>,
    out_schema: SchemaRef,
    /// Outer child's schema — used for `concat_batches` so an empty outer
    /// stream still yields a correctly-typed (zero-row) batch.
    outer_schema: SchemaRef,
    /// Inner child's schema — used so an empty inner stream null-shapes (rather
    /// than erroring) instead of having no schema to concat against.
    inner_schema: SchemaRef,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum OptionalJoinKey {
    U64(u64),
    Uuid([u8; 16]),
}

/// Run the LEFT OUTER join with null-shaping and build the output batch.
///
/// Output columns = all outer columns, then the inner columns named by
/// [`inner_keep_idx`](OptionalConfig::inner_keep_idx) (every shared-variable
/// column excluded — those come from the outer side), in that order. Unmatched
/// outer rows get a null index into the inner side, which `take` materialises as
/// null in every inner output column (the node's schema marks them nullable).
fn optional_join(
    cfg: &OptionalConfig,
    outer_batches: &[RecordBatch],
    inner_batches: &[RecordBatch],
) -> Result<RecordBatch, GfError> {
    use std::collections::HashMap;

    use arrow::array::{Array, FixedSizeBinaryArray, UInt32Array, UInt64Array};
    use arrow::compute::{concat_batches, take};

    let exec_err = |m: String| GfError::Execution(m);

    // Use the child schemas (not the joined output schema). `concat_batches`
    // returns a correctly-typed empty batch for an empty slice, so a child that
    // yields zero batches is handled: an empty inner null-shapes every outer
    // row rather than erroring.
    let outer =
        concat_batches(&cfg.outer_schema, outer_batches).map_err(|e| exec_err(e.to_string()))?;
    let inner =
        concat_batches(&cfg.inner_schema, inner_batches).map_err(|e| exec_err(e.to_string()))?;

    let outer_key_idx: Vec<usize> = cfg.join_keys.iter().map(|&(o, _)| o).collect();
    let inner_key_idx: Vec<usize> = cfg.join_keys.iter().map(|&(_, i)| i).collect();

    // Index the inner side by its key tuple (skip rows with any null key — a
    // null join key never matches in openCypher).
    let key_tuple = |batch: &RecordBatch,
                     columns: &[usize],
                     row: usize|
     -> Result<Option<Vec<OptionalJoinKey>>, GfError> {
        columns
            .iter()
            .map(|&index| {
                let column = batch.column(index);
                if matches!(column.data_type(), arrow::datatypes::DataType::Null)
                    || column.is_null(row)
                {
                    return Ok(None);
                }
                if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
                    Ok(Some(OptionalJoinKey::U64(values.value(row))))
                } else if let Some(values) = column.as_any().downcast_ref::<FixedSizeBinaryArray>()
                {
                    Ok(Some(OptionalJoinKey::Uuid(
                        values.value(row).try_into().map_err(|_| {
                            exec_err(format!(
                                "optional join key at index {index} is not a 16-byte UUID"
                            ))
                        })?,
                    )))
                } else {
                    Err(exec_err(format!(
                        "optional join key at index {index} has unsupported type {:?}",
                        column.data_type()
                    )))
                }
            })
            .collect::<Result<Option<Vec<_>>, _>>()
    };
    let mut index: HashMap<Vec<OptionalJoinKey>, Vec<usize>> = HashMap::new();
    for r in 0..inner.num_rows() {
        if let Some(k) = key_tuple(&inner, &inner_key_idx, r)? {
            index.entry(k).or_default().push(r);
        }
    }

    // Build take-index vectors. outer_take always points at a real outer row;
    // inner_take is a null slot for unmatched rows (→ null in every inner col).
    let mut outer_take: Vec<u32> = Vec::new();
    let mut inner_take: Vec<Option<u32>> = Vec::new();
    let to_u32 = |v: usize| -> Result<u32, GfError> {
        u32::try_from(v).map_err(|_| exec_err(format!("row index {v} exceeds u32")))
    };
    for o in 0..outer.num_rows() {
        let matches = if cfg.join_keys.is_empty() {
            // Unconditional join: every inner row matches every outer row.
            (0..inner.num_rows()).collect::<Vec<_>>()
        } else {
            key_tuple(&outer, &outer_key_idx, o)?
                .and_then(|k| index.get(&k).cloned())
                .unwrap_or_default()
        };
        if matches.is_empty() {
            outer_take.push(to_u32(o)?);
            inner_take.push(None);
        } else {
            for m in matches {
                outer_take.push(to_u32(o)?);
                inner_take.push(Some(to_u32(m)?));
            }
        }
    }

    let outer_take = UInt32Array::from(outer_take);
    let inner_take = UInt32Array::from(inner_take); // None entries → null indices

    let mut columns: Vec<arrow::array::ArrayRef> =
        Vec::with_capacity(cfg.out_schema.fields().len());
    for col in outer.columns() {
        columns.push(take(col, &outer_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    // Append only the kept inner columns (shared-variable columns excluded — they
    // are carried by the outer side), in the node's declared order.
    let inner_cols = inner.columns();
    for &i in &cfg.inner_keep_idx {
        let col = inner_cols.get(i).ok_or_else(|| {
            exec_err(format!(
                "OptionalMatch inner_keep_idx {i} out of range ({} inner columns)",
                inner_cols.len()
            ))
        })?;
        columns.push(take(col, &inner_take, None).map_err(|e| exec_err(e.to_string()))?);
    }

    RecordBatch::try_new(cfg.out_schema.clone(), columns).map_err(|e| exec_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// UnwindExec — physical node for UNWIND
// ---------------------------------------------------------------------------

/// Physical execution node for `UNWIND`, the physical counterpart of
/// [`UnwindNode`].
///
/// For each input row, evaluates `list_expr` to a list and emits one output row
/// per element — the input columns plus a new `alias` column bound to the
/// element. A null or empty list yields **zero** rows for that input row
/// (the input row is dropped), matching openCypher.
pub struct UnwindExec {
    input: Arc<dyn ExecutionPlan>,
    list_expr: DfExpr,
    input_schema: SchemaRef,
    /// The logical input's QUALIFIED schema (`var_0.name`, …). `list_expr`
    /// references qualified columns, so it must be planned against this — NOT a
    /// `DFSchema` rebuilt from the physical Arrow schema, whose field names are
    /// unqualified (`name`) and would fail to resolve `var_0.name` (#599/#28).
    input_dfschema: datafusion::common::DFSchemaRef,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl UnwindExec {
    /// Build the physical node from its logical counterpart and planned input.
    #[must_use]
    pub fn new(node: &UnwindNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let input_schema = input.schema();
        Self {
            input,
            list_expr: node.list_expr.clone(),
            input_schema,
            input_dfschema: node.input.schema().clone(),
            schema,
            props,
        }
    }
}

impl fmt::Debug for UnwindExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnwindExec {{ list_expr: {} }}", self.list_expr)
    }
}

impl DisplayAs for UnwindExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnwindExec: list={}", self.list_expr)
    }
}

impl ExecutionPlan for UnwindExec {
    fn name(&self) -> &str {
        "UnwindExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("UnwindExec needs one child".into()))?;
        // Derive the input schema from the NEW child (not the stale self value):
        // an optimizer may replace the child with a different-schema plan, and
        // unwind_explode concats/evaluates against this schema.
        let input_schema = input.schema();
        Ok(Arc::new(Self {
            input,
            list_expr: self.list_expr.clone(),
            input_schema,
            // Qualifiers + column order are preserved across child replacement, so
            // the original qualified DFSchema stays valid for resolving list_expr.
            input_dfschema: self.input_dfschema.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "UnwindExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let cfg = UnwindConfig {
            list_expr: self.list_expr.clone(),
            input_schema: self.input_schema.clone(),
            input_dfschema: self.input_dfschema.clone(),
            out_schema: self.schema.clone(),
        };
        let schema = self.schema.clone();
        let fut = async move {
            let input_batches = collect(input, context).await?;
            unwind_explode(&cfg, &input_batches).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut),
        )))
    }
}

/// Owned config for [`unwind_explode`] (so it can run in a `'static` future).
struct UnwindConfig {
    list_expr: DfExpr,
    input_schema: SchemaRef,
    input_dfschema: datafusion::common::DFSchemaRef,
    out_schema: SchemaRef,
}

/// Evaluate `list_expr` per input row and explode the resulting list into one
/// output row per element (input columns + the element column).
fn unwind_explode(
    cfg: &UnwindConfig,
    input_batches: &[RecordBatch],
) -> Result<RecordBatch, GfError> {
    use arrow::array::{ListArray, UInt32Array};
    use arrow::compute::{concat, take};

    let exec_err = |m: String| GfError::Execution(m);

    let input = arrow::compute::concat_batches(&cfg.input_schema, input_batches)
        .map_err(|e| exec_err(e.to_string()))?;

    // Evaluate the list expression against the input batch → one list per row.
    // Plan it against the QUALIFIED logical input schema: `list_expr` references
    // qualified columns (`var_0.name`), which a `DFSchema` rebuilt from the
    // physical Arrow schema (unqualified field names) cannot resolve (#599/#28).
    // `create_physical_expr` resolves them to column indices, which align with the
    // physical batch (same column order).
    let phys = create_physical_expr(&cfg.list_expr, &cfg.input_dfschema, &ExecutionProps::new())
        .map_err(|e| exec_err(e.to_string()))?;
    let list_values = phys
        .evaluate(&input)
        .and_then(|cv| cv.into_array(input.num_rows()))
        .map_err(|e| exec_err(e.to_string()))?;

    if matches!(list_values.data_type(), arrow::datatypes::DataType::Null) {
        let columns = cfg
            .out_schema
            .fields()
            .iter()
            .map(|field| arrow::array::new_empty_array(field.data_type()))
            .collect();
        return RecordBatch::try_new(cfg.out_schema.clone(), columns)
            .map_err(|e| exec_err(e.to_string()));
    }

    let lists = list_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            exec_err(format!(
                "UNWIND expression must evaluate to a list, got {:?}",
                list_values.data_type()
            ))
        })?;

    // For each input row, repeat its index once per list element (null/empty
    // list → contributes nothing), and collect the element sub-arrays.
    let mut input_take: Vec<u32> = Vec::new();
    let mut element_parts: Vec<arrow::array::ArrayRef> = Vec::new();
    for row in 0..input.num_rows() {
        if lists.is_null(row) {
            continue; // null list → zero rows
        }
        let elems = lists.value(row); // the element sub-array for this row
        let n = elems.len();
        if n == 0 {
            continue; // empty list → zero rows
        }
        let row_u32 =
            u32::try_from(row).map_err(|_| exec_err(format!("row index {row} exceeds u32")))?;
        input_take.extend(std::iter::repeat_n(row_u32, n));
        element_parts.push(elems);
    }

    let input_take = UInt32Array::from(input_take);
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(input.num_columns() + 1);
    for col in input.columns() {
        columns.push(take(col, &input_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    // The element column is the concatenation of every row's element sub-array,
    // already aligned with `input_take` (which repeats each row per element).
    let element_col: arrow::array::ArrayRef = if element_parts.is_empty() {
        // No elements at all — build an empty array of the output element type.
        let field = cfg.out_schema.field(cfg.out_schema.fields().len() - 1);
        arrow::array::new_empty_array(field.data_type())
    } else {
        let refs: Vec<&dyn Array> = element_parts.iter().map(AsRef::as_ref).collect();
        concat(&refs).map_err(|e| exec_err(e.to_string()))?
    };
    if cfg.out_schema.fields().len() == input.num_columns() + 1 {
        columns.push(element_col);
    } else {
        let values = element_col
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .ok_or_else(|| {
                exec_err(format!(
                    "UNWIND entity output requires a struct element, got {:?}",
                    element_col.data_type()
                ))
            })?;
        columns.extend(values.columns().iter().cloned());
    }

    RecordBatch::try_new(cfg.out_schema.clone(), columns).map_err(|e| exec_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// ExtensionPlanner + QueryPlanner
// ---------------------------------------------------------------------------

/// `SessionConfig` extension carrying the session's adjacency provider
/// (#761). Config extensions are keyed by `TypeId`, so a concrete newtype is
/// required to carry the trait object from [`ExecutionSession`] to
/// [`GraphForgeExtensionPlanner`].
pub struct AdjacencyProviderExt(pub Arc<dyn AdjacencyProvider>);

/// `SessionConfig` extension carrying the facade's exact generation-pinned
/// ordinal identity authority.
struct OrdinalIdentityResolverExt(pub Option<Arc<V4OrdinalIdentitySession>>);

fn plan_expand_extension(
    expand: &graphforge_plan::ExpandNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    session_state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let input = physical_inputs
        .first()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("Expand requires one physical input".into()))?;
    let provider = session_state
        .config()
        .get_extension::<AdjacencyProviderExt>()
        .map_or_else(
            || {
                Arc::new(ScanBuildAdjacencyProvider::new(
                    expand.dir.clone(),
                    expand.mode,
                )) as Arc<dyn AdjacencyProvider>
            },
            |ext| Arc::clone(&ext.0),
        );
    let identity_extension = session_state
        .config()
        .get_extension::<OrdinalIdentityResolverExt>();
    let ordinal_identity_required = identity_extension.is_some();
    let ordinal_identities = identity_extension
        .as_ref()
        .and_then(|extension| extension.0.as_ref().map(Arc::clone));
    Ok(Arc::new(ExpandExec::new(
        expand,
        input,
        provider,
        ordinal_identities,
        ordinal_identity_required,
    )))
}

/// Plans GraphForge's custom logical [`Extension`](LogicalPlan::Extension)
/// nodes into physical [`ExecutionPlan`]s.
#[derive(Debug, Default)]
struct GraphForgeExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for GraphForgeExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(create) = node.as_any().downcast_ref::<GraphCreateNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphCreate requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(GraphCreateExec::new(create, input))));
        }
        if let Some(delete) = node.as_any().downcast_ref::<GraphDeleteNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphDelete requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(GraphDeleteExec::new(delete, input))));
        }
        if let Some(set) = node.as_any().downcast_ref::<GraphSetNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphSet requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(GraphSetExec::new(set, input))));
        }
        if let Some(remove) = node.as_any().downcast_ref::<GraphRemoveNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("GraphRemove requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(GraphRemoveExec::new(remove, input))));
        }
        if let Some(expand) = node.as_any().downcast_ref::<graphforge_plan::ExpandNode>() {
            return plan_expand_extension(expand, physical_inputs, session_state).map(Some);
        }
        if let Some(var_len) = node.as_any().downcast_ref::<VarLenExpandNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("VarLenExpand requires one physical input".into())
            })?;
            // The session-scoped provider (#761) travels via SessionConfig
            // extension; a foreign SessionState without one falls back to a
            // fresh scan-build provider (today's pre-index behavior).
            let provider = session_state
                .config()
                .get_extension::<AdjacencyProviderExt>()
                .map_or_else(
                    || {
                        Arc::new(ScanBuildAdjacencyProvider::new(
                            var_len.dir.clone(),
                            var_len.mode,
                        )) as Arc<dyn AdjacencyProvider>
                    },
                    |ext| Arc::clone(&ext.0),
                );
            return Ok(Some(Arc::new(VarLenExpandExec::new(
                var_len, input, provider,
            ))));
        }
        if let Some(opt) = node.as_any().downcast_ref::<OptionalMatchNode>() {
            // inputs() order is [outer, optional], so physical_inputs matches.
            let (Some(outer), Some(inner)) = (
                physical_inputs.first().cloned(),
                physical_inputs.get(1).cloned(),
            ) else {
                return Err(DataFusionError::Internal(
                    "OptionalMatch requires two physical inputs".into(),
                ));
            };
            return Ok(Some(Arc::new(OptionalMatchExec::new(opt, outer, inner))));
        }
        if let Some(unwind) = node.as_any().downcast_ref::<UnwindNode>() {
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("Unwind requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(UnwindExec::new(unwind, input))));
        }
        if let Some(infer) = node
            .as_any()
            .downcast_ref::<graphforge_plan::OntologyInferNode>()
        {
            // Pass-through (#605): the wrapped var-len input computes the closure;
            // this carries the inference rule_id into the physical plan/explain().
            let input = physical_inputs.first().cloned().ok_or_else(|| {
                DataFusionError::Internal("OntologyInfer requires one physical input".into())
            })?;
            return Ok(Some(Arc::new(OntologyInferExec::new(infer, input))));
        }
        Ok(None)
    }
}

/// Custom DataFusion query planner that knows how to physically plan
/// GraphForge's graph-native logical nodes.
#[derive(Debug, Default)]
pub struct GraphForgeQueryPlanner;

#[async_trait]
impl QueryPlanner for GraphForgeQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            GraphForgeExtensionPlanner,
        )]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

// ---------------------------------------------------------------------------
// ExecutionSession
// ---------------------------------------------------------------------------

/// Resource knobs applied when constructing an [`ExecutionSession`] (#337).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionResourceConfig {
    /// DataFusion `target_partitions`.
    pub target_partitions: usize,
    /// DataFusion batch size.
    pub batch_size: usize,
    /// Memory pool budget in bytes.
    pub memory_budget_bytes: u64,
    /// Whether spill-to-disk is enabled.
    pub spill_enabled: bool,
    /// Absolute spill directory when spill is enabled.
    pub spill_directory: Option<PathBuf>,
    /// Optional spill byte cap.
    pub spill_max_bytes: Option<u64>,
    /// Concurrent Parquet / I/O open budget (#337 / #339).
    pub io_concurrency: usize,
}

impl Default for SessionResourceConfig {
    fn default() -> Self {
        Self {
            target_partitions: 2,
            batch_size: 8_192,
            memory_budget_bytes: 512 * 1024 * 1024,
            spill_enabled: false,
            spill_directory: None,
            spill_max_bytes: None,
            io_concurrency: 2,
        }
    }
}

/// A configured DataFusion [`SessionContext`] ready to execute [`GraphPlan`]s.
///
/// Construct via [`ExecutionSession::new`] (read/query) or
/// [`ExecutionSession::new_with_target`] (write execution, e.g. `CREATE`).
///
/// # Thread safety
///
/// `ExecutionSession` is `Send + Sync`.
pub struct ExecutionSession {
    ctx: SessionContext,
    /// The graph catalog, retained so read lowering can decide typed-vs-
    /// exploratory edge tables. The same `Arc` is also registered on `ctx`.
    catalog: Arc<GraphCatalog>,
    ontology: Option<OntologyHandle>,
    /// Project directory for write execution; empty for read-only sessions.
    dir: PathBuf,
    /// Ontology mode driving write routing.
    mode: OntologyMode,
    /// Exact composition fingerprint authenticating semantic write routes.
    semantic_composition_fingerprint: Option<String>,
    /// The session's adjacency provider (also registered as a SessionConfig
    /// extension for the planner). Held concretely so successful writes can
    /// invalidate its memoized state and loaded views — a same-session
    /// read → write → read must observe post-write adjacency.
    adjacency_provider: Arc<PersistentAdjacencyProvider>,
    /// Doc-hidden differential-test strategy; production sessions leave false.
    relational_fixed_hop_reference: bool,
}

#[derive(Default)]
struct OrdinalIdentityConfig {
    session: Option<Arc<V4OrdinalIdentitySession>>,
    required: bool,
}

impl ExecutionSession {
    /// Create a read/query session.
    ///
    /// Write execution (`CREATE`) requires a project directory — use
    /// [`new_with_target`](Self::new_with_target) for that.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new(catalog: GraphCatalog, ontology: Option<OntologyHandle>) -> Result<Self, GfError> {
        Ok(Self::build(
            catalog,
            ontology,
            PathBuf::new(),
            OntologyMode::Exploratory,
            None,
            OrdinalIdentityConfig::default(),
            &SessionResourceConfig::default(),
        ))
    }

    /// Create a session that can execute writes against `dir`.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new_with_target(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Result<Self, GfError> {
        Ok(Self::build(
            catalog,
            ontology,
            dir,
            mode,
            None,
            OrdinalIdentityConfig::default(),
            &SessionResourceConfig::default(),
        ))
    }

    /// Like [`new_with_target`](Self::new_with_target) but reusing a
    /// long-lived adjacency provider (#832): the facade owns one per
    /// `GraphForge` instance so loaded CSR views amortize across queries.
    /// The provider is [`revalidate`](PersistentAdjacencyProvider::revalidate)d
    /// at construction, so each session still observes external index/topology
    /// changes.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new_with_target_and_provider(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        provider: Arc<PersistentAdjacencyProvider>,
    ) -> Result<Self, GfError> {
        Self::new_with_target_provider_and_resources(
            catalog,
            ontology,
            dir,
            mode,
            provider,
            &SessionResourceConfig::default(),
        )
    }

    /// Like [`new_with_target_and_provider`] with an explicit resource policy.
    ///
    /// # Errors
    /// Returns [`GfError`] if session construction fails.
    pub fn new_with_target_provider_and_resources(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        provider: Arc<PersistentAdjacencyProvider>,
        resources: &SessionResourceConfig,
    ) -> Result<Self, GfError> {
        Self::new_with_target_provider_resources_and_identity(
            catalog, ontology, dir, mode, provider, None, resources,
        )
    }

    /// Like [`Self::new_with_target_provider_and_resources`] with an exact
    /// generation-pinned destination identity authority.
    pub fn new_with_target_provider_resources_and_identity(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        provider: Arc<PersistentAdjacencyProvider>,
        ordinal_identities: Option<Arc<V4OrdinalIdentityResolver>>,
        resources: &SessionResourceConfig,
    ) -> Result<Self, GfError> {
        let identity = match ordinal_identities {
            Some(resolver) => OrdinalIdentityConfig {
                session: resolver.pin()?,
                // A missing handle is a legitimate pre-v4 generation. A
                // generation that declares v4 but cannot admit it is rejected
                // while opening the authenticated handle, before execution.
                required: false,
            },
            None => OrdinalIdentityConfig::default(),
        };
        Ok(Self::build(
            catalog,
            ontology,
            dir,
            mode,
            Some(provider),
            identity,
            resources,
        ))
    }

    fn build(
        catalog: GraphCatalog,
        ontology: Option<OntologyHandle>,
        dir: PathBuf,
        mode: OntologyMode,
        shared_provider: Option<Arc<PersistentAdjacencyProvider>>,
        identity: OrdinalIdentityConfig,
        resources: &SessionResourceConfig,
    ) -> Self {
        // The session-scoped adjacency provider (#761), threaded to the
        // extension planner via SessionConfig extension. Read-only sessions
        // (empty dir) need no special case: with no `indexes/adjacency/`
        // capability dir the provider degrades to scan-build by itself.
        // A facade-shared provider (#832) is revalidated here — once per
        // session — so its memoized state is as fresh as a per-query
        // provider's, while loaded views amortize across queries.
        let adjacency_provider = shared_provider.map_or_else(
            || Arc::new(PersistentAdjacencyProvider::new(dir.clone(), mode)),
            |p| {
                p.revalidate();
                p
            },
        );
        let provider: Arc<dyn AdjacencyProvider> = Arc::clone(&adjacency_provider) as _;
        let mut config = datafusion::prelude::SessionConfig::new()
            .with_extension(Arc::new(AdjacencyProviderExt(provider)))
            .with_extension(Arc::new(graphforge_storage::IoConcurrencyExt::new(
                resources.io_concurrency,
            )))
            .with_target_partitions(resources.target_partitions)
            .with_batch_size(resources.batch_size);
        if identity.session.is_some() || identity.required {
            config = config.with_extension(Arc::new(OrdinalIdentityResolverExt(identity.session)));
        }
        // Authenticated overlay scans publish sound physical-row upper bounds.
        // Let DataFusion use those estimates so a small one-partition source is
        // not eagerly repartitioned merely because newest-wins makes its exact
        // logical cardinality unavailable without executing the scan.
        config
            .options_mut()
            .execution
            .use_row_number_estimates_to_optimize_partitioning = true;

        let memory_budget = usize::try_from(resources.memory_budget_bytes).unwrap_or(usize::MAX);
        let mut runtime_builder = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_limit(memory_budget, 1.0);
        if resources.spill_enabled
            && let Some(dir) = &resources.spill_directory
        {
            let _ = std::fs::create_dir_all(dir);
            runtime_builder = runtime_builder.with_temp_file_path(dir.clone());
            if let Some(max) = resources.spill_max_bytes {
                runtime_builder = runtime_builder.with_max_temp_directory_size(max);
            }
        }
        let runtime_env = Arc::new(
            runtime_builder
                .build()
                .expect("DataFusion RuntimeEnv construction"),
        );

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_query_planner(Arc::new(GraphForgeQueryPlanner))
            // Runs after DataFusion's default rules, when terminal fetches and
            // eager round-robin exchanges are visible (#1269).
            .with_physical_optimizer_rule(Arc::new(demand::FixedHopDemandRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        let semantic_composition_fingerprint = catalog
            .semantic_composition_fingerprint()
            .map(str::to_owned);
        let catalog = Arc::new(catalog);
        ctx.register_catalog("graph", catalog.clone());
        Self {
            ctx,
            catalog,
            ontology,
            dir,
            mode,
            semantic_composition_fingerprint,
            adjacency_provider,
            relational_fixed_hop_reference: false,
        }
    }

    /// Use the relational fixed-hop implementation as an independent test
    /// oracle. The public `GraphForge` facade never enables this strategy.
    #[doc(hidden)]
    #[must_use]
    pub fn with_relational_fixed_hop_reference(mut self) -> Self {
        self.relational_fixed_hop_reference = true;
        self
    }

    /// Execute a `CREATE` [`GraphPlan`] and return the write summary.
    ///
    /// Lowers the plan (resolving CREATE specs against the session's ontology
    /// and write target), physically plans it through the custom
    /// [`ExtensionPlanner`], and collects the resulting summary batch.
    ///
    /// # Errors
    /// Returns [`GfError`] if the session has no write target, or if lowering /
    /// planning / execution fails.
    pub async fn execute_create(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_create_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `CREATE` [`GraphPlan`] with `$name` parameters available to a
    /// terminal `RETURN` / projection suffix.
    ///
    /// The write specs themselves are already bound into the graph plan; params
    /// matter for expressions that remain in the logical plan, such as
    /// `CREATE (n {name: 'Apa'}) RETURN n[$idx]`.
    pub async fn execute_create_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        if !plan
            .ops
            .iter()
            .any(|op| matches!(op, graphforge_ir::GraphOp::Merge { .. }))
        {
            return self.execute_write_statement_with_params(plan, params).await;
        }
        if self.dir.as_os_str().is_empty() {
            return Err(GfError::Execution(
                "execute_create requires a write target; build the session with new_with_target"
                    .into(),
            ));
        }

        // Pass the catalog: although CREATE has no scans, the lowerer resolves
        // each edge's relation-type name from it (the ontology map alone is
        // empty in exploratory mode, where relation names live in the runtime
        // catalog). Without it, edges are written with a `_UNKNOWN` relation
        // name and a later `MATCH ()-[:REL]->()` filter never matches.
        let lowerer = GraphPlanLowerer::new_for_writes(
            Some(&self.catalog),
            self.ontology.as_ref(),
            &self.dir,
            self.mode,
        );
        let logical = bind_query_params(lowerer.lower_plan(plan)?, params)?;

        let physical = self
            .ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(|e| GfError::Plan(e.to_string()))?;

        let batches = collect(Arc::clone(&physical), self.ctx.task_ctx())
            .await
            .map_err(|e| GfError::Execution(e.to_string()))?;

        let schema = batches
            .first()
            .map_or_else(GraphCreateNode::summary_schema, RecordBatch::schema);
        let rows_produced = batches.iter().map(|b| b.num_rows() as u64).sum();
        // In write-result RETURN (emit-rows) mode the `batches` are the created
        // rows, not the summary — so the side-effect counts come from the exec's
        // tally (found by walking the executed plan). Otherwise read the summary.
        let side_effects = Some(create_tally_in_plan(&physical).map_or_else(
            || SideEffects::from_summary(&batches),
            |t| SideEffects {
                nodes_created: t.nodes_created,
                relationships_created: t.edges_created,
                properties_set: t.properties_set,
                labels_added: t.labels_added,
                ..SideEffects::default()
            },
        ));
        self.adjacency_provider.invalidate();
        Ok(ExecutionResult {
            schema,
            batches,
            stats: ExecutionStats {
                rows_produced,
                execution_time_ms: 0,
            },
            side_effects,
            mutation_receipt: None,
        })
    }

    /// Execute a `DELETE` / `DETACH DELETE` [`GraphPlan`] and return the
    /// one-row write summary (#740).
    ///
    /// Like [`execute_create`](Self::execute_create), this requires a write
    /// target (build the session via `new_with_target`); the lowerer opens the
    /// write path via `new_for_writes` and the physical `GraphDeleteExec` drives
    /// the storage rewrite primitives, enforcing the no-`DETACH` relationship
    /// rule.
    ///
    /// # Errors
    /// Returns [`GfError`] if there is no write target, or if lowering, physical
    /// planning, or execution fails (including the no-`DETACH` error).
    pub async fn execute_delete(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_delete_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `DELETE` / `DETACH DELETE` [`GraphPlan`] with `$name` query
    /// parameters available to the read prefix and delete target expressions.
    ///
    /// # Errors
    /// As [`execute_delete`](Self::execute_delete).
    pub async fn execute_delete_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, params).await
    }

    /// Execute a `SET` [`GraphPlan`] and return the one-row write summary
    /// (`properties_set`) (#791).
    ///
    /// # Errors
    /// Returns [`GfError`] if there is no write target, or if lowering, physical
    /// planning, or execution fails.
    pub async fn execute_set(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_set_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `SET` [`GraphPlan`] with `$name` query parameters available to
    /// the read prefix and assigned expressions.
    ///
    /// # Errors
    /// As [`execute_set`](Self::execute_set).
    pub async fn execute_set_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, params).await
    }

    /// Execute a `REMOVE` [`GraphPlan`] and return the one-row write summary
    /// (`properties_removed`) (#791).
    ///
    /// # Errors
    /// Returns [`GfError`] if there is no write target, or if lowering, physical
    /// planning, or execution fails.
    pub async fn execute_remove(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_remove_with_params(plan, &HashMap::new()).await
    }

    /// Execute a `REMOVE` [`GraphPlan`] with `$name` query parameters available
    /// to the read prefix.
    ///
    /// # Errors
    /// As [`execute_remove`](Self::execute_remove).
    pub async fn execute_remove_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, params).await
    }

    /// Execute a write statement in clause order through the unified driver
    /// (#817): one read prefix runs once, then each write clause
    /// applies against the shared frontier (extended with CREATE-minted
    /// variables), and every file effect commits as a single staged batch.
    /// Returns the one-row six-counter summary.
    ///
    /// # Errors
    /// Returns [`GfError`] for lowering, execution, or commit failures.
    pub async fn execute_write_statement(
        &self,
        plan: &GraphPlan,
    ) -> Result<ExecutionResult, GfError> {
        self.execute_write_statement_with_params(plan, &HashMap::new())
            .await
    }

    /// Execute any supported write statement through the clause-ordered driver
    /// with query parameters available to prefix and value expressions.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_write_statement_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        if self.dir.as_os_str().is_empty() {
            return Err(GfError::Execution(
                "write statements require a write target; build the session with new_with_target"
                    .into(),
            ));
        }
        let split = write_driver::split_write_plan(&plan.ops)?;
        let lowerer = GraphPlanLowerer::new_for_writes(
            Some(&self.catalog),
            self.ontology.as_ref(),
            &self.dir,
            self.mode,
        );

        // Run the read prefix once, keeping the variable registrations the
        // write phases resolve against.
        let mut var_map = graphforge_rel::VarMap::new();
        let logical =
            lowerer.lower_prefix(&plan.ops[..split.prefix_len], &plan.exprs, &mut var_map)?;
        let logical = bind_query_params(logical, params)?;
        let df_schema = logical.schema().as_ref().clone();
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(|e| GfError::Plan(e.to_string()))?;
        let batches = collect(physical, self.ctx.task_ctx())
            .await
            .map_err(|e| GfError::Execution(e.to_string()))?;
        let mut frontier = write_driver::Frontier { df_schema, batches };

        // Phase loop: buffer every effect, then one staged commit.
        let env = write_driver::PhaseEnv {
            lowerer: &lowerer,
            exprs: &plan.exprs,
            dir: &self.dir,
            mode: self.mode,
            params,
            type_map: lowerer.entity_name_map(),
        };
        let mut wctx = write_driver::StatementWriteContext::new(&self.dir, self.mode)?
            .with_semantic_composition_fingerprint(self.semantic_composition_fingerprint.clone());
        let create_retention =
            write_driver::create_retention_by_write(&plan.ops, &plan.exprs, &split);
        let mut cursor = split.prefix_len;
        for &write_index in &split.write_ops {
            if cursor < write_index {
                run_write_relational_segment(
                    &self.ctx,
                    &lowerer,
                    plan,
                    cursor..write_index,
                    &mut frontier,
                    &mut var_map,
                    params,
                    &wctx.writer.pending_nodes_batch()?,
                )
                .await?;
            }
            write_driver::run_write_phases(
                &env,
                &plan.ops,
                &[write_index],
                &mut frontier,
                &mut var_map,
                &mut wctx,
                create_retention
                    .as_ref()
                    .and_then(|retention| retention.get(&write_index)),
            )?;
            cursor = write_index + 1;
        }
        Self::validate_deleted_entity_projection(
            plan,
            split.read_suffix_start,
            !wctx.deleted.is_empty(),
        )?;
        let terminal_logical = lower_write_terminal_suffix(
            &lowerer,
            plan,
            split.read_suffix_start,
            &frontier,
            &var_map,
            params,
            &wctx.writer.pending_nodes_batch()?,
        )?;
        drop(env);
        drop(lowerer);
        let terminal_result = match terminal_logical {
            Some(logical) => {
                Some(write_driver::run_terminal_suffix(&self.ctx, &logical, &frontier).await?)
            }
            None => None,
        };
        write_driver::commit_statement(&mut wctx, &self.dir)?;
        self.catalog
            .refresh_property_inventory(&self.dir)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        self.adjacency_provider.invalidate();

        let c = wctx.counters;
        let mutation_receipt = Some(wctx.mutation_receipt());
        let side_effects = Some(SideEffects {
            nodes_created: c.nodes_created,
            nodes_deleted: c.nodes_deleted,
            relationships_created: c.edges_created,
            relationships_deleted: c.edges_deleted,
            properties_set: c.properties_set,
            properties_removed: c.properties_removed,
            labels_added: c.labels_added,
            labels_removed: c.labels_removed,
        });
        if let Some((schema, batches, rows_produced)) = terminal_result {
            return Ok(ExecutionResult {
                schema,
                batches,
                stats: ExecutionStats {
                    rows_produced,
                    execution_time_ms: 0,
                },
                side_effects,
                mutation_receipt,
            });
        }

        let batch = write_driver::statement_summary_batch(&wctx.counters)?;
        Ok(ExecutionResult {
            schema: batch.schema(),
            batches: vec![batch],
            stats: ExecutionStats {
                rows_produced: 1,
                execution_time_ms: 0,
            },
            side_effects,
            mutation_receipt,
        })
    }

    fn validate_deleted_entity_projection(
        plan: &GraphPlan,
        suffix_start: Option<usize>,
        any_entity_deleted: bool,
    ) -> Result<(), GfError> {
        let Some(suffix_start) = suffix_start.filter(|_| any_entity_deleted) else {
            return Ok(());
        };
        let deleted: HashSet<VarId> = plan.ops[..suffix_start]
            .iter()
            .filter_map(|op| match op {
                GraphOp::Delete { vars, .. } => Some(vars.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect();
        if deleted.is_empty() {
            return Ok(());
        }
        let references_deleted = plan.ops[suffix_start..].iter().any(|op| match op {
            GraphOp::Project { items, .. } | GraphOp::With { items, .. } => items
                .iter()
                .any(|item| Self::expr_accesses_deleted_vars(&plan.exprs, item.expr, &deleted)),
            GraphOp::Filter { predicate } => {
                Self::expr_accesses_deleted_vars(&plan.exprs, *predicate, &deleted)
            }
            GraphOp::Sort { keys } => keys
                .iter()
                .any(|key| Self::expr_accesses_deleted_vars(&plan.exprs, key.expr, &deleted)),
            GraphOp::Aggregate { group_by, aggs, .. } => {
                group_by
                    .iter()
                    .any(|expr| Self::expr_accesses_deleted_vars(&plan.exprs, *expr, &deleted))
                    || aggs.iter().any(|agg| {
                        agg.arg.is_some_and(|expr| {
                            Self::expr_accesses_deleted_vars(&plan.exprs, expr, &deleted)
                        })
                    })
            }
            _ => false,
        });
        if references_deleted {
            return Err(GfError::Execution(
                "DeletedEntityAccess: cannot access an entity after it has been deleted".into(),
            ));
        }
        Ok(())
    }

    fn expr_accesses_deleted_vars(arena: &ExprArena, id: ExprId, vars: &HashSet<VarId>) -> bool {
        match arena.get(id) {
            IrExpr::PropertyAccess { base, .. } => Self::expr_references_vars(arena, *base, vars),
            IrExpr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("type") => false,
            IrExpr::FunctionCall { name, args }
                if matches!(
                    name.as_str(),
                    "labels" | "properties" | "_node_struct" | "_rel_struct"
                ) =>
            {
                args.iter()
                    .any(|expr| Self::expr_references_vars(arena, *expr, vars))
            }
            IrExpr::FunctionCall { args, .. } | IrExpr::ListLiteral(args) => args
                .iter()
                .any(|expr| Self::expr_accesses_deleted_vars(arena, *expr, vars)),
            IrExpr::BinaryOp { left, right, .. } => {
                Self::expr_accesses_deleted_vars(arena, *left, vars)
                    || Self::expr_accesses_deleted_vars(arena, *right, vars)
            }
            IrExpr::UnaryOp { expr, .. } => Self::expr_accesses_deleted_vars(arena, *expr, vars),
            IrExpr::MapLiteral(entries) => entries
                .iter()
                .any(|(_, expr)| Self::expr_accesses_deleted_vars(arena, *expr, vars)),
            _ => false,
        }
    }

    fn expr_references_vars(arena: &ExprArena, id: ExprId, vars: &HashSet<VarId>) -> bool {
        match arena.get(id) {
            IrExpr::VarRef(var) => vars.contains(var),
            IrExpr::PropertyAccess { base, .. } => Self::expr_references_vars(arena, *base, vars),
            IrExpr::BinaryOp { left, right, .. } => {
                Self::expr_references_vars(arena, *left, vars)
                    || Self::expr_references_vars(arena, *right, vars)
            }
            IrExpr::UnaryOp { expr, .. } => Self::expr_references_vars(arena, *expr, vars),
            IrExpr::FunctionCall { args, .. } | IrExpr::ListLiteral(args) => args
                .iter()
                .any(|expr| Self::expr_references_vars(arena, *expr, vars)),
            IrExpr::MapLiteral(entries) => entries
                .iter()
                .any(|(_, expr)| Self::expr_references_vars(arena, *expr, vars)),
            IrExpr::Case {
                operand,
                arms,
                else_expr,
            } => {
                operand.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
                    || arms.iter().any(|arm| {
                        Self::expr_references_vars(arena, arm.when, vars)
                            || Self::expr_references_vars(arena, arm.then, vars)
                    })
                    || else_expr.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
            }
            IrExpr::Quantifier {
                list, predicate, ..
            } => {
                Self::expr_references_vars(arena, *list, vars)
                    || Self::expr_references_vars(arena, *predicate, vars)
            }
            IrExpr::ListComprehension {
                list,
                filter,
                projection,
                ..
            } => {
                Self::expr_references_vars(arena, *list, vars)
                    || filter.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
                    || projection.is_some_and(|expr| Self::expr_references_vars(arena, expr, vars))
            }
            IrExpr::Literal(_) | IrExpr::Parameter(_) => false,
        }
    }

    /// Execute a read [`GraphPlan`] and return the result.
    ///
    /// Equivalent to [`execute_plan_with_params`](Self::execute_plan_with_params)
    /// with no parameters.
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering, physical planning, or execution fails.
    pub async fn execute_plan(&self, plan: &GraphPlan) -> Result<ExecutionResult, GfError> {
        self.execute_plan_with_params(plan, &HashMap::new()).await
    }

    /// Execute a read [`GraphPlan`], substituting `$name` placeholders with the
    /// supplied parameter values, and return the result.
    ///
    /// Lowers the plan to a DataFusion `LogicalPlan` (scans bound to the
    /// project's Parquet-backed catalog tables), replaces any query-parameter
    /// placeholders with the values in `params` (by name; a placeholder with no
    /// provided value errors), physically plans it through the custom
    /// [`GraphForgeQueryPlanner`], and collects the resulting batches.
    /// `CREATE` plans should use [`execute_create`](Self::execute_create).
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering, parameter binding, physical planning, or
    /// execution fails.
    pub async fn execute_plan_with_params(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        let resolved_plan = self.resolve_row_count_expressions(plan, params).await?;
        let (physical, fallback_schema) = self.plan_physical(&resolved_plan, params).await?;

        let mut batches = collect(Arc::clone(&physical), self.ctx.task_ctx())
            .await
            .map_err(|e| GfError::Execution(e.to_string()))?;

        // DataFusion's collect may return zero batches for an empty stream.
        // Public callers (and DF54-era optimistic publish tests) index
        // `batches[0]` for schema/row counts; mirror the write-path terminal
        // suffix and always surface one empty batch with the plan schema.
        let schema = batches
            .first()
            .map_or_else(|| fallback_schema, RecordBatch::schema);
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(Arc::clone(&schema)));
        }
        let rows_produced = batches.iter().map(|b| b.num_rows() as u64).sum();
        Ok(ExecutionResult {
            schema,
            batches,
            stats: ExecutionStats {
                rows_produced,
                execution_time_ms: 0,
            },
            side_effects: None,
            mutation_receipt: None,
        })
    }

    async fn resolve_row_count_expressions(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<GraphPlan, GfError> {
        let mut resolved = plan.clone();
        for op in &mut resolved.ops {
            match op {
                GraphOp::Optional { child }
                | GraphOp::Exists { child, .. }
                | GraphOp::PatternComprehension { child, .. }
                | GraphOp::ListElementPatternComprehension { child, .. } => {
                    **child = Box::pin(self.resolve_row_count_expressions(child, params)).await?;
                }
                GraphOp::Union { inputs, .. } => {
                    for input in inputs {
                        *input =
                            Box::pin(self.resolve_row_count_expressions(input, params)).await?;
                    }
                }
                _ => {}
            }
        }
        let exprs = resolved.exprs.clone();
        for op in &mut resolved.ops {
            let (keyword, expr) = match op {
                GraphOp::SkipExpr { expr } => ("SKIP", *expr),
                GraphOp::LimitExpr { expr } => ("LIMIT", *expr),
                _ => continue,
            };
            let vars = graphforge_rel::VarMap::new();
            let lowerer = graphforge_rel::ExprLowerer::new(&exprs, self.ontology.as_ref(), &vars);
            let expr = lowerer
                .lower(expr)
                .map_err(|error| GfError::Plan(error.to_string()))?;
            let logical = LogicalPlanBuilder::empty(true)
                .project(vec![expr.alias("__gf_row_count")])
                .and_then(LogicalPlanBuilder::build)
                .map_err(|error| GfError::Plan(error.to_string()))?;
            let logical = bind_query_params(logical, params)?;
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&logical)
                .await
                .map_err(|error| GfError::Plan(error.to_string()))?;
            let batches = collect(physical, self.ctx.task_ctx())
                .await
                .map_err(|error| GfError::Execution(error.to_string()))?;
            let batch = batches.first().ok_or_else(|| {
                GfError::Execution(format!("{keyword} expression returned no row"))
            })?;
            let value = ScalarValue::try_from_array(batch.column(0), 0)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            let count = match value {
                ScalarValue::Int64(Some(value)) => u64::try_from(value).ok(),
                ScalarValue::UInt64(Some(value)) => Some(value),
                ScalarValue::Int32(Some(value)) => u64::try_from(value).ok(),
                ScalarValue::UInt32(Some(value)) => Some(u64::from(value)),
                _ => None,
            }
            .ok_or_else(|| {
                GfError::Execution(format!(
                    "{keyword} expression must evaluate to a non-negative integer"
                ))
            })?;
            *op = match keyword {
                "SKIP" => GraphOp::Skip { count },
                _ => GraphOp::Limit { count },
            };
        }
        Ok(resolved)
    }

    /// Execute a read [`GraphPlan`] and return a lazy stream of result batches,
    /// substituting `$name` placeholders with `params`.
    ///
    /// The streaming counterpart of
    /// [`execute_plan_with_params`](Self::execute_plan_with_params): it builds
    /// the same physical plan but drives it incrementally via DataFusion's
    /// [`execute_stream`] rather than collecting eagerly. Output shaping
    /// (UUID-only columns, schema metadata) is applied by the caller per batch.
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering, parameter binding, or physical planning
    /// fails. Per-batch execution errors surface on the returned stream.
    pub async fn execute_plan_stream(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<SendableRecordBatchStream, GfError> {
        let (physical, _) = self.plan_physical(plan, params).await?;
        datafusion::physical_plan::execute_stream(physical, self.ctx.task_ctx())
            .map_err(|e| GfError::Execution(e.to_string()))
    }

    /// Render the physical plan for a [`GraphPlan`] (indented, one line per
    /// node) without executing it.
    ///
    /// Write plans (`CREATE` / `MERGE` / `DELETE` / `SET` / `REMOVE`) lower via
    /// [`GraphPlanLowerer::new_for_writes`] and stop after
    /// `create_physical_plan` — never `collect` — so EXPLAIN shows the write
    /// path without publishing mutations. Read plans reuse [`Self::plan_physical`].
    ///
    /// This is the physical-plan inspection surface: node lines carry
    /// execution detail the logical stages cannot show, such as
    /// `adjacency=hit | miss | building` on traversal nodes (#761).
    ///
    /// # Errors
    /// Returns [`GfError`] if lowering or physical planning fails.
    pub async fn explain_physical(&self, plan: &GraphPlan) -> Result<String, GfError> {
        if plan_requires_writes(plan) {
            if self.dir.as_os_str().is_empty() {
                return Err(GfError::Execution(
                    "explain of a write plan requires a write target; \
                     build the session with new_with_target"
                        .into(),
                ));
            }
            let lowerer = GraphPlanLowerer::new_for_writes(
                Some(&self.catalog),
                self.ontology.as_ref(),
                &self.dir,
                self.mode,
            );
            let logical = lowerer.lower_plan(plan)?;
            let physical = self
                .ctx
                .state()
                .create_physical_plan(&logical)
                .await
                .map_err(|e| GfError::Plan(e.to_string()))?;
            return Ok(datafusion::physical_plan::displayable(physical.as_ref())
                .indent(false)
                .to_string());
        }
        let (physical, _) = self.plan_physical(plan, &HashMap::new()).await?;
        Ok(datafusion::physical_plan::displayable(physical.as_ref())
            .indent(false)
            .to_string())
    }

    /// Lower `plan`, bind `$name` parameters, and build the physical plan —
    /// shared by the collecting and streaming read paths. Returns the physical
    /// plan and the logical plan's schema (a fallback for an empty result).
    async fn plan_physical(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, graphforge_ir::IrLiteral>,
    ) -> Result<(Arc<dyn ExecutionPlan>, SchemaRef), GfError> {
        // Read lowering always needs the catalog (typed-vs-exploratory edge
        // routing). Scans additionally bind their real Parquet-backed providers
        // from the project directory. A read-only session built via `new` has an
        // empty `dir`: binding a scan there would resolve a CWD-relative path
        // like `topology/nodes.parquet`, silently reading the wrong file. Such a
        // session cannot read persisted data, so reject scan plans up front
        // (mirroring `execute_create`'s write-target guard) and lower the rest
        // schema-only so pure computed/`RETURN` plans still run.
        let mut lowerer = if self.dir.as_os_str().is_empty() {
            if plan_reads_persisted_data(plan) {
                return Err(GfError::Execution(
                    "execute_plan requires a project directory to read persisted nodes/edges; \
                     build the session with new_with_target"
                        .into(),
                ));
            }
            GraphPlanLowerer::new(Some(&self.catalog), self.ontology.as_ref())
        } else {
            GraphPlanLowerer::new_with_dir(
                Some(&self.catalog),
                self.ontology.as_ref(),
                &self.dir,
                self.mode,
            )
        };
        if self.relational_fixed_hop_reference {
            lowerer = lowerer.with_relational_fixed_hop_reference();
        }
        let logical = lowerer.lower_plan(plan)?;

        // Bind `$name` query parameters to their values, replacing the
        // DataFusion placeholders with literals. Skipped when there are no
        // params (the common read path) — `with_param_values` would otherwise
        // walk the plan needlessly; with params, a placeholder lacking a value
        // surfaces as a clear `GfError::Plan`.
        let logical = bind_query_params(logical, params)?;

        let fallback_schema = physical_schema_fallback(&logical);
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .map_err(|e| GfError::Plan(e.to_string()))?;
        Ok((physical, fallback_schema))
    }

    /// Return a reference to the underlying DataFusion [`SessionContext`].
    #[must_use]
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

fn lower_write_terminal_suffix(
    lowerer: &GraphPlanLowerer<'_>,
    plan: &GraphPlan,
    start: Option<usize>,
    frontier: &write_driver::Frontier,
    var_map: &graphforge_rel::VarMap,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
    pending_nodes: &RecordBatch,
) -> Result<Option<Box<LogicalPlan>>, GfError> {
    let Some(start) = start else {
        return Ok(None);
    };
    let mut suffix_vars = var_map.clone();
    let logical = lowerer.lower_write_segment(
        &plan.ops[start..],
        &plan.exprs,
        &mut suffix_vars,
        Arc::new(frontier.df_schema.clone()),
        pending_nodes,
    )?;
    Ok(Some(Box::new(bind_query_params(logical, params)?)))
}

#[allow(clippy::too_many_arguments)]
async fn run_write_relational_segment(
    session: &SessionContext,
    lowerer: &GraphPlanLowerer<'_>,
    plan: &GraphPlan,
    range: std::ops::Range<usize>,
    frontier: &mut write_driver::Frontier,
    var_map: &mut graphforge_rel::VarMap,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
    pending_nodes: &RecordBatch,
) -> Result<(), GfError> {
    let logical = lowerer.lower_write_segment(
        &plan.ops[range],
        &plan.exprs,
        var_map,
        Arc::new(frontier.df_schema.clone()),
        pending_nodes,
    )?;
    let logical = bind_query_params(logical, params)?;
    let df_schema = logical.schema().as_ref().clone();
    let (_, batches, _) = write_driver::run_terminal_suffix(session, &logical, frontier).await?;
    frontier.df_schema = df_schema;
    frontier.batches = batches;
    Ok(())
}

fn bind_query_params(
    logical: LogicalPlan,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
) -> Result<LogicalPlan, GfError> {
    // Always apply param substitution — including an empty map — so unbound
    // `$name` placeholders fail at plan time instead of depending on whether
    // the optimizer happens to evaluate the expression (empty-graph scans can
    // otherwise skip the placeholder and silently return zero rows).
    let values: HashMap<String, datafusion::scalar::ScalarValue> = params
        .iter()
        .map(|(name, lit)| (name.clone(), graphforge_rel::ir_literal_to_scalar(lit)))
        .collect();
    logical
        .with_param_values(values)
        .map_err(|e| GfError::Plan(e.to_string()))
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
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryBuilder, Int64Array};
    use arrow::datatypes::Schema;
    use datafusion::logical_expr::{Extension, LogicalPlanBuilder, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use graphforge_ir::RuntimeCatalog;
    use graphforge_plan::{
        GraphDeleteNode, GraphRemoveNode, GraphSetNode, RemoveTarget, SetTarget,
    };
    use tempfile::TempDir;

    fn make_session() -> ExecutionSession {
        let dir = TempDir::new().unwrap();
        let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
        ExecutionSession::new(catalog, None).unwrap()
    }

    #[test]
    fn unused_list_output_preserves_exact_non_nullable_child_schema() {
        let item = Arc::new(Field::new("item", DataType::UInt32, false));
        let field = Field::new("type_ids", DataType::List(Arc::clone(&item)), false);
        let column = unused_expand_column(&field, 3).unwrap();

        assert_eq!(column.data_type(), field.data_type());
        assert_eq!(column.len(), 3);
        assert_eq!(column.null_count(), 0);
    }

    #[test]
    fn session_uses_sound_row_estimates_for_partition_planning() {
        let session = make_session();
        assert!(
            session
                .ctx
                .state()
                .config_options()
                .execution
                .use_row_number_estimates_to_optimize_partitioning
        );
    }

    #[test]
    fn persisted_read_detection_recurses_through_every_nested_plan_shape() {
        let scan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(1),
                ty: None,
            })
            .build();
        let empty = GraphPlan::builder("openCypher").build();
        assert!(plan_reads_persisted_data(&scan));
        assert!(!plan_reads_persisted_data(&empty));

        let nested = [
            GraphOp::Optional {
                child: Box::new(scan.clone()),
            },
            GraphOp::Exists {
                child: Box::new(scan.clone()),
                negated: false,
            },
            GraphOp::PatternComprehension {
                child: Box::new(scan.clone()),
                output: VarId(2),
            },
            GraphOp::ListElementPatternComprehension {
                list_expr: ExprId(0),
                loop_var: VarId(3),
                child: Box::new(scan.clone()),
                pattern_output: VarId(4),
                filter: None,
                projection: None,
                output: VarId(5),
            },
            GraphOp::Union {
                all: true,
                inputs: vec![empty.clone(), scan.clone()],
            },
        ];
        for op in nested {
            let plan = GraphPlan::builder("openCypher").push_op(op).build();
            assert!(plan_reads_persisted_data(&plan));
        }
        let union = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Union {
                all: false,
                inputs: vec![empty],
            })
            .build();
        assert!(!plan_reads_persisted_data(&union));
    }

    #[test]
    fn create_writer_covers_typed_edges_properties_recording_and_persisted_lookup() {
        use graphforge_core::uuid::{new_v7, to_bytes};
        use graphforge_plan::{ResolvedEdgeSpec, ResolvedNodeSpec};

        let dir = TempDir::new().unwrap();
        let arrow_schema = Arc::new(Schema::empty());
        let input = RecordBatch::try_new_with_options(
            Arc::clone(&arrow_schema),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();
        let nodes = vec![
            ResolvedNodeSpec {
                var: 1,
                label_ids: vec![7],
                label_names: vec!["Person".into()],
                properties: vec![("name".into(), IrLiteral::Str("Alice".into()))],
                computed_properties: vec![],
                is_reference: false,
            },
            ResolvedNodeSpec {
                var: 2,
                label_ids: vec![7, 8],
                label_names: vec!["Person".into(), "Employee".into()],
                properties: vec![("missing".into(), IrLiteral::Null)],
                computed_properties: vec![],
                is_reference: false,
            },
        ];
        let edge = ResolvedEdgeSpec {
            var: 3,
            src: 1,
            dst: 2,
            rel_type_id: Some(9),
            rel_type_name: Some("KNOWS".into()),
            direction: graphforge_ir::Direction::In,
            properties: vec![("since".into(), IrLiteral::Int(2020))],
            computed_properties: vec![],
        };
        let cfg = CreateConfig {
            semantic_composition_fingerprint: None,
            nodes: nodes.clone(),
            edges: vec![edge.clone()],
            ref_cols: vec![],
            in_df_schema: Arc::new(DFSchema::empty()),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            out_schema: Arc::clone(&arrow_schema),
        };
        validate_edge_specs(&cfg).unwrap();
        assert!(build_ref_by_var(&cfg).is_empty());

        let mut writer =
            graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1)
                .unwrap();
        let mut recorder = write_driver::CreateRecorder::default();
        let mut tally = CreateTally::default();
        write_batch_creates(
            &cfg,
            &mut writer,
            &input,
            &HashMap::new(),
            CreateExtras {
                recorder: Some(&mut recorder),
                ..CreateExtras::default()
            },
            &mut tally,
        )
        .unwrap();
        assert_eq!((tally.nodes_created, tally.edges_created), (4, 2));
        assert_eq!(tally.properties_set, 4);
        assert_eq!(distinct_created_labels(&nodes, tally.nodes_created), 2);
        assert_eq!(recorder.node_identities(1).unwrap().0.len(), 2);
        writer.flush().unwrap();
        assert_eq!(persisted_node_ids(dir.path()).unwrap().len(), 4);

        let invalid_untyped = CreateConfig {
            semantic_composition_fingerprint: None,
            nodes: nodes.clone(),
            edges: vec![ResolvedEdgeSpec {
                rel_type_id: None,
                rel_type_name: None,
                ..edge.clone()
            }],
            ref_cols: vec![],
            in_df_schema: Arc::new(DFSchema::empty()),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            out_schema: Arc::clone(&arrow_schema),
        };
        assert!(
            validate_edge_specs(&invalid_untyped)
                .unwrap_err()
                .to_string()
                .contains("relationship type")
        );
        let invalid_undirected = CreateConfig {
            semantic_composition_fingerprint: None,
            nodes,
            edges: vec![ResolvedEdgeSpec {
                direction: graphforge_ir::Direction::Undirected,
                ..edge
            }],
            ref_cols: vec![],
            in_df_schema: Arc::new(DFSchema::empty()),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            out_schema: arrow_schema,
        };
        assert!(
            validate_edge_specs(&invalid_undirected)
                .unwrap_err()
                .to_string()
                .contains("undirected")
        );

        let known = new_v7();
        let mut writer =
            graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 2)
                .unwrap();
        writer.create_node_with_labels(known, &[]).unwrap();
        writer.flush().unwrap();
        assert!(
            persisted_node_ids(dir.path())
                .unwrap()
                .contains_key(&to_bytes(&known))
        );
    }

    #[test]
    fn create_identity_and_emit_helpers_reject_every_incomplete_shape() {
        use graphforge_plan::ResolvedNodeSpec;

        let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(2, 16);
        uuid_builder.append_value([1; 16]).unwrap();
        uuid_builder.append_null();
        let uuid_array = Arc::new(uuid_builder.finish()) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("node_uuid", Arc::clone(&uuid_array))]).unwrap();
        let cols = RefNodeCols {
            var: 4,
            uuid_idx: 0,
            uuid_child_idx: None,
            node_id_idx: Some(0),
        };
        assert_eq!(
            referenced_node_uuid(&batch, &cols, 0).unwrap().as_bytes(),
            &[1; 16]
        );
        assert!(
            referenced_node_uuid(&batch, &cols, 1)
                .unwrap_err()
                .to_string()
                .contains("null")
        );

        let non_struct = RecordBatch::try_from_iter([(
            "entity",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let nested = RefNodeCols {
            var: 5,
            uuid_idx: 0,
            uuid_child_idx: Some(0),
            node_id_idx: None,
        };
        assert!(
            referenced_node_uuid(&non_struct, &nested, 0)
                .unwrap_err()
                .to_string()
                .contains("not a struct")
        );

        let spec = ResolvedNodeSpec {
            var: 7,
            label_ids: vec![1],
            label_names: vec!["Person".into()],
            properties: vec![],
            computed_properties: vec![],
            is_reference: false,
        };
        let mut out = Vec::new();
        assert_eq!(
            append_created_node_output_cols(
                &spec,
                1,
                &CreateComputed::new(),
                &write_driver::CreateRecorder::default(),
                &mut out,
            )
            .unwrap_err()
            .to_string(),
            "execution error: emit-rows CREATE did not record identities for var 7"
        );

        let mut recorder = write_driver::CreateRecorder::default();
        recorder.record_node(7, [1; 16], 1, 1);
        assert!(
            append_created_node_output_cols(&spec, 2, &CreateComputed::new(), &recorder, &mut out,)
                .unwrap_err()
                .to_string()
                .contains("incomplete emitted identities")
        );

        let computed = HashMap::from([(
            7,
            vec![(
                "score".into(),
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            )],
        )]);
        assert!(
            append_created_node_output_cols(&spec, 1, &computed, &recorder, &mut out)
                .unwrap_err()
                .to_string()
                .contains("computed property column")
        );
    }

    #[test]
    fn create_writer_fails_closed_for_unbound_or_unpersisted_references() {
        use graphforge_plan::{ResolvedEdgeSpec, ResolvedNodeSpec};

        let dir = TempDir::new().unwrap();
        let schema = Arc::new(Schema::empty());
        let empty = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let reference = ResolvedNodeSpec {
            var: 1,
            label_ids: vec![],
            label_names: vec![],
            properties: vec![],
            computed_properties: vec![],
            is_reference: true,
        };
        let mut writer =
            graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1)
                .unwrap();
        let cfg = CreateConfig {
            semantic_composition_fingerprint: None,
            nodes: vec![reference.clone()],
            edges: vec![],
            ref_cols: vec![],
            in_df_schema: Arc::new(DFSchema::empty()),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            out_schema: Arc::clone(&schema),
        };
        let error = write_batch_creates(
            &cfg,
            &mut writer,
            &empty,
            &HashMap::new(),
            CreateExtras::default(),
            &mut CreateTally::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not found in the input schema"));

        let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
        uuid_builder.append_value([7; 16]).unwrap();
        let uuid = Arc::new(uuid_builder.finish()) as ArrayRef;
        let node_ids = Arc::new(UInt64Array::from(vec![None])) as ArrayRef;
        let bound =
            RecordBatch::try_from_iter([("node_uuid", Arc::clone(&uuid)), ("node_id", node_ids)])
                .unwrap();
        let cols = RefNodeCols {
            var: 1,
            uuid_idx: 0,
            uuid_child_idx: None,
            node_id_idx: Some(1),
        };
        let refs = HashMap::from([(1, &cols)]);
        let error = write_batch_creates(
            &cfg,
            &mut writer,
            &bound,
            &refs,
            CreateExtras::default(),
            &mut CreateTally::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("node_id is null"));

        let node_ids = Arc::new(UInt64Array::from(vec![Some(99)])) as ArrayRef;
        let uuid_only =
            RecordBatch::try_from_iter([("node_uuid", uuid), ("node_id", node_ids)]).unwrap();
        let refs = HashMap::from([(1, &cols)]);
        let deleted = HashSet::from([[7; 16]]);
        let error = write_batch_creates(
            &cfg,
            &mut writer,
            &uuid_only,
            &refs,
            CreateExtras {
                deleted: Some(&deleted),
                ..CreateExtras::default()
            },
            &mut CreateTally::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("deleted earlier"));

        let edge = ResolvedEdgeSpec {
            var: 3,
            src: 1,
            dst: 2,
            rel_type_id: Some(9),
            rel_type_name: Some("KNOWS".into()),
            direction: graphforge_ir::Direction::Out,
            properties: vec![],
            computed_properties: vec![],
        };
        let mut edge_cfg = CreateConfig {
            semantic_composition_fingerprint: None,
            nodes: vec![],
            edges: vec![edge.clone()],
            ref_cols: vec![],
            in_df_schema: Arc::new(DFSchema::empty()),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            out_schema: Arc::clone(&schema),
        };
        let error = write_batch_creates(
            &edge_cfg,
            &mut writer,
            &empty,
            &HashMap::new(),
            CreateExtras::default(),
            &mut CreateTally::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unbound src"));
        edge_cfg.nodes.push(ResolvedNodeSpec {
            var: 1,
            is_reference: false,
            ..reference
        });
        let error = write_batch_creates(
            &edge_cfg,
            &mut writer,
            &empty,
            &HashMap::new(),
            CreateExtras::default(),
            &mut CreateTally::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unbound dst"));
    }

    #[test]
    fn set_and_remove_batch_contracts_route_rows_and_skip_null_identities() {
        let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(3, 16);
        uuid_builder.append_value([1; 16]).unwrap();
        uuid_builder.append_null();
        uuid_builder.append_value([2; 16]).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("node_uuid", Arc::new(uuid_builder.finish()) as ArrayRef),
            (
                "type_id",
                Arc::new(arrow::array::UInt32Array::from(vec![7, 7, 8])) as ArrayRef,
            ),
        ])
        .unwrap();
        let df_schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
        let target = WriteCol {
            prop_name: "score".into(),
            uuid_idx: 0,
            is_edge: false,
            type_id_idx: Some(1),
            rel_name_idx: None,
        };
        let logical_value = lit(42_i64);
        let physical_value =
            create_physical_expr(&logical_value, &df_schema, &ExecutionProps::new()).unwrap();
        let type_map = HashMap::from([(7, "Person".into()), (8, "Employee".into())]);

        let mut set = SetAccumulator::default();
        accumulate_set_batch(
            &batch,
            &[(target.clone(), logical_value)],
            &[physical_value],
            OntologyMode::Strict,
            &type_map,
            &mut set,
        )
        .unwrap();
        assert_eq!(set.nodes["Person"].len(), 1);
        assert_eq!(set.nodes["Employee"].len(), 1);
        assert!(!set.nodes.values().any(|rows| rows.contains_key(&[0; 16])));

        let mut remove = RemoveAccumulator::default();
        accumulate_remove_batch(
            &batch,
            &[target],
            OntologyMode::Strict,
            &type_map,
            &mut remove,
        )
        .unwrap();
        assert_eq!(remove.nodes["Person"].len(), 1);
        assert_eq!(remove.nodes["Employee"].len(), 1);
    }

    #[test]
    fn emit_rows_create_runs_the_writer_and_shapes_created_identity_columns() {
        use graphforge_plan::ResolvedNodeSpec;

        let dir = TempDir::new().unwrap();
        let input_schema = Arc::new(Schema::empty());
        let input = RecordBatch::try_new_with_options(
            Arc::clone(&input_schema),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let item = Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::UInt32,
            false,
        ));
        let output_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new(
                "node_uuid",
                arrow::datatypes::DataType::FixedSizeBinary(16),
                false,
            ),
            arrow::datatypes::Field::new("node_id", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("type_id", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("type_ids", arrow::datatypes::DataType::List(item), false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let cfg = CreateConfig {
            semantic_composition_fingerprint: None,
            nodes: vec![ResolvedNodeSpec {
                var: 1,
                label_ids: vec![7],
                label_names: vec!["Person".into()],
                properties: vec![("name".into(), IrLiteral::Str("Ada".into()))],
                computed_properties: vec![],
                is_reference: false,
            }],
            edges: vec![],
            ref_cols: vec![],
            in_df_schema: Arc::new(DFSchema::empty()),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            out_schema: output_schema,
        };
        let mut writer =
            graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1)
                .unwrap();
        let mut tally = CreateTally::default();

        let emitted = emit_batch_creates(
            &cfg,
            &mut writer,
            &input,
            &CreateComputed::new(),
            &HashMap::new(),
            None,
            &mut tally,
        )
        .unwrap();

        assert_eq!(emitted.num_rows(), 1);
        assert_eq!(emitted.num_columns(), 5);
        assert_eq!((tally.nodes_created, tally.properties_set), (1, 1));
    }

    #[tokio::test]
    async fn graph_create_exec_emits_rows_records_effects_and_preserves_empty_input() {
        use datafusion_datasource::memory::MemorySourceConfig;
        use graphforge_plan::ResolvedNodeSpec;

        let run = |rows: usize| async move {
            let dir = TempDir::new().unwrap();
            // Keep a physical column in the frontier: DataFusion's in-memory
            // source normalizes a zero-column batch to zero rows.
            let input_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "frontier",
                arrow::datatypes::DataType::UInt32,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(arrow::array::UInt32Array::from_iter_values(
                    0..rows as u32,
                ))],
            )
            .unwrap();
            let physical =
                MemorySourceConfig::try_new_from_batches(Arc::clone(&input_schema), vec![batch])
                    .unwrap();
            let logical = Arc::new(LogicalPlanBuilder::empty(false).build().unwrap());
            let item = Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::UInt32,
                false,
            ));
            let output_schema = Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("frontier", arrow::datatypes::DataType::UInt32, false),
                arrow::datatypes::Field::new(
                    "node_uuid",
                    arrow::datatypes::DataType::FixedSizeBinary(16),
                    false,
                ),
                arrow::datatypes::Field::new("node_id", arrow::datatypes::DataType::UInt64, false),
                arrow::datatypes::Field::new("type_id", arrow::datatypes::DataType::UInt32, false),
                arrow::datatypes::Field::new(
                    "type_ids",
                    arrow::datatypes::DataType::List(item),
                    false,
                ),
                arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            ]));
            let node = GraphCreateNode::new_emitting(
                logical,
                vec![ResolvedNodeSpec {
                    var: 1,
                    label_ids: vec![7],
                    label_names: vec!["Person".into()],
                    properties: vec![("name".into(), IrLiteral::Str("Ada".into()))],
                    computed_properties: vec![],
                    is_reference: false,
                }],
                vec![],
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
                Arc::new(DFSchema::try_from(output_schema.as_ref().clone()).unwrap()),
            );
            let exec = Arc::new(GraphCreateExec::new(&node, physical));
            assert!(exec.emits_rows());
            let batches = collect(exec.clone(), SessionContext::new().task_ctx())
                .await
                .unwrap();
            let physical: Arc<dyn ExecutionPlan> = exec.clone();
            let discovered = create_tally_in_plan(&physical).unwrap();
            assert_eq!(discovered.nodes_created, exec.effects().nodes_created);
            assert_eq!(discovered.properties_set, exec.effects().properties_set);
            (batches, exec.effects(), dir)
        };

        let (batches, effects, project) = run(1).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!((effects.nodes_created, effects.properties_set), (1, 1));
        assert_eq!(persisted_node_ids(project.path()).unwrap().len(), 1);

        let (empty, effects, _) = run(0).await;
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].num_rows(), 0);
        assert_eq!(
            (
                effects.nodes_created,
                effects.edges_created,
                effects.properties_set,
                effects.labels_added,
            ),
            (0, 0, 0, 0)
        );
    }

    fn empty_write_input() -> (Arc<LogicalPlan>, Arc<dyn ExecutionPlan>) {
        let logical = Arc::new(LogicalPlanBuilder::empty(false).build().unwrap());
        let physical: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(
            logical.schema().as_arrow().clone(),
        )));
        (logical, physical)
    }

    #[test]
    fn write_summary_reads_named_counters_and_ignores_missing_or_empty_rows() {
        let batch = RecordBatch::try_from_iter([
            (
                "properties_set",
                Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            ),
            (
                "nodes_created",
                Arc::new(UInt64Array::from(vec![2])) as ArrayRef,
            ),
            (
                "edges_deleted",
                Arc::new(UInt64Array::from(vec![3])) as ArrayRef,
            ),
        ])
        .unwrap();

        assert_eq!(
            SideEffects::from_summary(&[batch]),
            SideEffects {
                nodes_created: 2,
                relationships_deleted: 3,
                properties_set: 7,
                ..SideEffects::default()
            }
        );
        assert_eq!(SideEffects::from_summary(&[]), SideEffects::default());
        assert_eq!(
            SideEffects::from_summary(&[RecordBatch::try_from_iter([(
                "nodes_created",
                Arc::new(UInt64Array::from(Vec::<u64>::new())) as ArrayRef,
            )])
            .unwrap()]),
            SideEffects::default()
        );
    }

    #[test]
    fn referenced_node_columns_resolve_qualified_unqualified_and_struct_shapes() {
        use datafusion::arrow::datatypes::{Field, Fields};
        use datafusion::common::TableReference;

        let qualified = DFSchema::new_with_metadata(
            vec![
                (
                    Some(TableReference::bare("var_2")),
                    Arc::new(Field::new(
                        "node_uuid",
                        DataType::FixedSizeBinary(16),
                        false,
                    )),
                ),
                (
                    Some(TableReference::bare("var_2")),
                    Arc::new(Field::new("node_id", DataType::UInt64, false)),
                ),
            ],
            HashMap::new(),
        )
        .unwrap();
        let resolved = RefNodeCols::resolve(&qualified, 2).unwrap();
        assert_eq!(
            (resolved.var, resolved.uuid_idx, resolved.node_id_idx),
            (2, 0, Some(1))
        );
        assert_eq!(resolved.uuid_child_idx, None);

        let unqualified = DFSchema::try_from(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("node_id", DataType::UInt64, false),
        ]))
        .unwrap();
        let resolved = RefNodeCols::resolve_with_alias(&unqualified, 4, "renamed").unwrap();
        assert_eq!(
            (resolved.var, resolved.uuid_idx, resolved.node_id_idx),
            (4, 0, Some(1))
        );

        let node_fields = Fields::from(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )]);
        let direct_struct = DFSchema::try_from(Schema::new(vec![Field::new(
            "entity",
            DataType::Struct(node_fields.clone()),
            true,
        )]))
        .unwrap();
        let resolved = RefNodeCols::resolve_with_alias(&direct_struct, 6, "entity").unwrap();
        assert_eq!(
            (
                resolved.uuid_idx,
                resolved.uuid_child_idx,
                resolved.node_id_idx
            ),
            (0, Some(0), None)
        );

        let dynamic_struct = DFSchema::try_from(Schema::new(vec![Field::new(
            "dynamic",
            DataType::Struct(Fields::from(vec![Field::new(
                "__het_value_8",
                DataType::Struct(node_fields),
                true,
            )])),
            true,
        )]))
        .unwrap();
        let resolved = RefNodeCols::resolve_struct_at(&dynamic_struct, 8, 0).unwrap();
        assert_eq!(resolved.uuid_child_idx, None);
        assert!(RefNodeCols::resolve_struct_at(&unqualified, 9, 0).is_none());
        assert!(RefNodeCols::resolve_with_alias(&direct_struct, 10, "missing").is_none());
    }

    #[test]
    fn optional_join_preserves_left_rows_matches_duplicate_keys_and_null_shapes_misses() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::Field;

        let outer_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, true),
            Field::new("outer", DataType::Utf8, false),
        ]));
        let inner_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, true),
            Field::new("inner", DataType::Int64, false),
        ]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, true),
            Field::new("outer", DataType::Utf8, false),
            Field::new("inner", DataType::Int64, true),
        ]));
        let outer = RecordBatch::try_new(
            outer_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![Some(1), Some(2), None])),
                Arc::new(StringArray::from(vec!["one", "two", "null"])),
            ],
        )
        .unwrap();
        let inner = RecordBatch::try_new(
            inner_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![Some(1), Some(1), None])),
                Arc::new(Int64Array::from(vec![10, 11, 99])),
            ],
        )
        .unwrap();
        let cfg = OptionalConfig {
            join_keys: vec![(0, 0)],
            inner_keep_idx: vec![1],
            out_schema,
            outer_schema,
            inner_schema,
        };

        let joined = optional_join(&cfg, &[outer], &[inner]).unwrap();
        assert_eq!(
            joined.num_rows(),
            4,
            "duplicate matches must duplicate the left row"
        );
        let keys = joined
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(
            keys.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(2), None]
        );
        let values = joined
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(10), Some(11), None, None]
        );
    }

    #[test]
    fn optional_join_handles_cartesian_empty_uuid_and_invalid_key_contracts() {
        use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
        use arrow::datatypes::Field;

        let outer_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let inner_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let outer = RecordBatch::try_new(
            outer_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let cfg = OptionalConfig {
            join_keys: vec![],
            inner_keep_idx: vec![0],
            out_schema: out_schema.clone(),
            outer_schema: outer_schema.clone(),
            inner_schema: inner_schema.clone(),
        };
        let empty = optional_join(&cfg, &[outer.clone()], &[]).unwrap();
        assert_eq!(empty.num_rows(), 2);
        assert!(empty.column(1).is_null(0) && empty.column(1).is_null(1));

        let inner = RecordBatch::try_new(
            inner_schema,
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .unwrap();
        let product = optional_join(&cfg, &[outer], &[inner]).unwrap();
        assert_eq!(product.num_rows(), 4);

        let uuid_schema = Arc::new(Schema::new(vec![Field::new(
            "uuid",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let uuids = FixedSizeBinaryArray::try_from_iter([&[1_u8; 16][..]].into_iter()).unwrap();
        let uuid_batch = RecordBatch::try_new(uuid_schema.clone(), vec![Arc::new(uuids)]).unwrap();
        let uuid_cfg = OptionalConfig {
            join_keys: vec![(0, 0)],
            inner_keep_idx: vec![],
            out_schema: uuid_schema.clone(),
            outer_schema: uuid_schema.clone(),
            inner_schema: uuid_schema,
        };
        assert_eq!(
            optional_join(&uuid_cfg, &[uuid_batch.clone()], &[uuid_batch])
                .unwrap()
                .num_rows(),
            1
        );

        let bad_key_cfg = OptionalConfig {
            join_keys: vec![(0, 0)],
            inner_keep_idx: vec![],
            out_schema: outer_schema.clone(),
            outer_schema: outer_schema.clone(),
            inner_schema: outer_schema.clone(),
        };
        let ints =
            RecordBatch::try_new(outer_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(
            optional_join(&bad_key_cfg, &[ints.clone()], &[ints])
                .unwrap_err()
                .to_string()
                .contains("unsupported type")
        );

        let bad_keep_cfg = OptionalConfig {
            inner_keep_idx: vec![9],
            ..cfg
        };
        let outer = RecordBatch::try_new(
            bad_keep_cfg.outer_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let inner = RecordBatch::try_new(
            bad_keep_cfg.inner_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["value"]))],
        )
        .unwrap();
        assert!(
            optional_join(&bad_keep_cfg, &[outer], &[inner])
                .unwrap_err()
                .to_string()
                .contains("inner_keep_idx 9 out of range")
        );
    }

    #[test]
    fn wave11_unwind_explode_preserves_order_and_enforces_list_contract() {
        use arrow::array::{Int64Array, ListArray, StringArray};
        use arrow::datatypes::Field;
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::col;

        let list_field = Arc::new(Field::new("item", DataType::Int64, true));
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("items", DataType::List(list_field), true),
        ]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("items", input_schema.field(1).data_type().clone(), true),
            Field::new("item", DataType::Int64, true),
        ]));
        let lists = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
            Some(vec![Some(3), Some(4)]),
            None,
            Some(vec![]),
            Some(vec![Some(8)]),
        ]);
        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(lists),
            ],
        )
        .unwrap();
        let cfg = UnwindConfig {
            list_expr: col("items"),
            input_schema: input_schema.clone(),
            input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
            out_schema,
        };
        let exploded = unwind_explode(&cfg, &[batch]).unwrap();
        assert_eq!(exploded.num_rows(), 3);
        assert_eq!(
            exploded
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 4, 8]
        );

        let scalar_schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::Int64,
            false,
        )]));
        let scalar_cfg = UnwindConfig {
            list_expr: col("items"),
            input_schema: scalar_schema.clone(),
            input_dfschema: Arc::new(DFSchema::try_from(scalar_schema.as_ref().clone()).unwrap()),
            out_schema: Arc::new(Schema::new(vec![
                Field::new("items", DataType::Int64, false),
                Field::new("item", DataType::Int64, true),
            ])),
        };
        let scalar =
            RecordBatch::try_new(scalar_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert!(
            unwind_explode(&scalar_cfg, &[scalar])
                .unwrap_err()
                .to_string()
                .contains("must evaluate to a list")
        );

        let entity_cfg = UnwindConfig {
            list_expr: col("items"),
            input_schema: input_schema.clone(),
            input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
            out_schema: Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("items", input_schema.field(1).data_type().clone(), true),
                Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("node_id", DataType::UInt64, true),
            ])),
        };
        let primitive_lists =
            ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([Some(vec![
                Some(1),
            ])]);
        let entity_input = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(primitive_lists),
            ],
        )
        .unwrap();
        assert!(
            unwind_explode(&entity_cfg, &[entity_input])
                .unwrap_err()
                .to_string()
                .contains("requires a struct element")
        );
    }

    #[test]
    fn wave11_low_level_expand_and_optional_schema_guards_fail_closed() {
        use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
        use arrow::datatypes::Field;

        let int_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let ints = RecordBatch::try_new(
            int_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(
            u64_column(&ints, 0)
                .unwrap_err()
                .to_string()
                .contains("UInt64")
        );
        assert!(
            string_column(&ints, "missing")
                .unwrap_err()
                .to_string()
                .contains("missing column")
        );
        assert!(
            string_column(&ints, "value")
                .unwrap_err()
                .to_string()
                .contains("Utf8")
        );

        let short_schema = Arc::new(Schema::new(vec![Field::new(
            "uuid",
            DataType::FixedSizeBinary(8),
            false,
        )]));
        let short = FixedSizeBinaryArray::try_from_iter([&[1_u8; 8][..]].into_iter()).unwrap();
        let short_batch =
            RecordBatch::try_new(short_schema.clone(), vec![Arc::new(short)]).unwrap();
        let optional = OptionalConfig {
            join_keys: vec![(0, 0)],
            inner_keep_idx: vec![],
            out_schema: short_schema.clone(),
            outer_schema: short_schema.clone(),
            inner_schema: short_schema,
        };
        assert!(
            optional_join(&optional, &[short_batch.clone()], &[short_batch])
                .unwrap_err()
                .to_string()
                .contains("not a 16-byte UUID")
        );

        let dir = TempDir::new().unwrap();
        let cfg = ExpandConfig {
            rel_type_name: "KNOWS".into(),
            direction: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
            dir: dir.path().to_path_buf(),
            mode: OntologyMode::Exploratory,
            src_col_idx: 0,
            out_schema: Arc::new(Schema::empty()),
            provider: Arc::new(PersistentAdjacencyProvider::new(
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            )),
        };
        assert!(
            build_edge_list_column(&cfg, &[], &HashMap::new())
                .unwrap_err()
                .to_string()
                .contains("no edge-list column")
        );
        let mut not_list = cfg;
        not_list.out_schema = int_schema;
        assert!(
            build_edge_list_column(&not_list, &[], &HashMap::new())
                .unwrap_err()
                .to_string()
                .contains("must be a List")
        );

        let utf8 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();
        assert!(u64_column(&utf8, 0).is_err());
    }

    #[test]
    fn write_execs_expose_stable_plan_contracts_and_reject_invalid_shape() {
        let dir = TempDir::new().unwrap();
        let (logical, physical) = empty_write_input();
        let create: Arc<dyn ExecutionPlan> = Arc::new(GraphCreateExec::new(
            &GraphCreateNode::new(
                logical.clone(),
                vec![],
                vec![],
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            ),
            physical.clone(),
        ));
        let delete: Arc<dyn ExecutionPlan> = Arc::new(GraphDeleteExec::new(
            &GraphDeleteNode::new(
                logical.clone(),
                vec![DeleteTarget {
                    var: 0,
                    is_edge: false,
                }],
                true,
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            ),
            physical.clone(),
        ));
        let set: Arc<dyn ExecutionPlan> = Arc::new(GraphSetExec::new(
            &GraphSetNode::new(
                logical.clone(),
                vec![SetTarget {
                    var: 0,
                    is_edge: false,
                    prop_name: "score".into(),
                    value: lit(1_i64),
                }],
                HashMap::new(),
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            ),
            physical.clone(),
        ));
        let remove: Arc<dyn ExecutionPlan> = Arc::new(GraphRemoveExec::new(
            &GraphRemoveNode::new(
                logical,
                vec![RemoveTarget {
                    var: 0,
                    is_edge: false,
                    prop_name: "score".into(),
                }],
                HashMap::new(),
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            ),
            physical,
        ));

        for (plan, expected_name) in [
            (create, "GraphCreateExec"),
            (delete, "GraphDeleteExec"),
            (set, "GraphSetExec"),
            (remove, "GraphRemoveExec"),
        ] {
            assert_eq!(plan.name(), expected_name);
            assert!(
                plan.is::<GraphCreateExec>()
                    || plan.is::<GraphDeleteExec>()
                    || plan.is::<GraphSetExec>()
                    || plan.is::<GraphRemoveExec>()
            );
            assert_eq!(plan.children().len(), 1);
            assert_eq!(plan.properties().output_partitioning().partition_count(), 1);
            assert!(format!("{plan:?}").contains(expected_name));
            assert!(
                datafusion::physical_plan::displayable(plan.as_ref())
                    .one_line()
                    .to_string()
                    .contains(expected_name)
            );
            let missing_child = plan.clone().with_new_children(vec![]).unwrap_err();
            assert!(missing_child.to_string().contains("needs one child"));
            let invalid_partition = match plan.execute(1, SessionContext::new().task_ctx()) {
                Ok(_) => panic!("{expected_name} accepted invalid partition 1"),
                Err(error) => error,
            };
            assert!(
                invalid_partition
                    .to_string()
                    .contains("only has partition 0")
            );
        }
    }

    #[tokio::test]
    async fn empty_write_execs_dispatch_and_report_zero_changes() {
        let dir = TempDir::new().unwrap();
        let (logical, physical) = empty_write_input();
        let plans: Vec<Arc<dyn ExecutionPlan>> = vec![
            Arc::new(GraphCreateExec::new(
                &GraphCreateNode::new(
                    logical.clone(),
                    vec![],
                    vec![],
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                ),
                physical.clone(),
            )),
            Arc::new(GraphDeleteExec::new(
                &GraphDeleteNode::new(
                    logical.clone(),
                    vec![],
                    false,
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                ),
                physical.clone(),
            )),
            Arc::new(GraphSetExec::new(
                &GraphSetNode::new(
                    logical.clone(),
                    vec![],
                    HashMap::new(),
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                ),
                physical.clone(),
            )),
            Arc::new(GraphRemoveExec::new(
                &GraphRemoveNode::new(
                    logical,
                    vec![],
                    HashMap::new(),
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                ),
                physical,
            )),
        ];

        for plan in plans {
            let batches =
                datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx())
                    .await
                    .unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            for column in batches[0].columns() {
                assert_eq!(
                    column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0),
                    0
                );
            }
        }
    }

    #[tokio::test]
    async fn delete_exec_enforces_bound_edge_and_detach_semantics() {
        use arrow::datatypes::Field;
        use datafusion_datasource::memory::MemorySourceConfig;
        use graphforge_core::uuid::{new_v7, to_bytes};

        let run = |detach: bool, include_edge: bool| async move {
            let dir = TempDir::new().unwrap();
            let node = new_v7();
            let other = new_v7();
            let edge = new_v7();
            let mut writer =
                graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1)
                    .unwrap();
            writer
                .create_node(node, graphforge_core::TypeId(1))
                .unwrap();
            writer
                .create_node(other, graphforge_core::TypeId(1))
                .unwrap();
            writer.create_edge(edge, "KNOWS", &node, &other).unwrap();
            writer.flush().unwrap();

            let schema = Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
            ]));
            let mut nodes = FixedSizeBinaryBuilder::with_capacity(1, 16);
            nodes.append_value(to_bytes(&node)).unwrap();
            let mut edges = FixedSizeBinaryBuilder::with_capacity(1, 16);
            edges.append_value(to_bytes(&edge)).unwrap();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(nodes.finish()), Arc::new(edges.finish())],
            )
            .unwrap();
            let input = MemorySourceConfig::try_new_from_batches(schema, vec![batch]).unwrap();
            let summary = GraphDeleteNode::summary_schema();
            let exec: Arc<dyn ExecutionPlan> = Arc::new(GraphDeleteExec {
                input,
                cols: if include_edge {
                    vec![
                        DeleteCol {
                            uuid_idx: 0,
                            is_edge: false,
                        },
                        DeleteCol {
                            uuid_idx: 1,
                            is_edge: true,
                        },
                    ]
                } else {
                    vec![DeleteCol {
                        uuid_idx: 0,
                        is_edge: false,
                    }]
                },
                detach,
                dir: dir.path().to_path_buf(),
                schema: Arc::clone(&summary),
                props: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(summary),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
            });
            (collect(exec, SessionContext::new().task_ctx()).await, dir)
        };

        let (error, _) = run(false, false).await;
        assert!(
            error
                .unwrap_err()
                .to_string()
                .contains("still has relationships")
        );

        let (batches, project) = run(false, true).await;
        let effects = SideEffects::from_summary(&batches.unwrap());
        assert_eq!(
            (effects.nodes_deleted, effects.relationships_deleted),
            (1, 1)
        );
        assert_eq!(persisted_node_ids(project.path()).unwrap().len(), 1);

        let (batches, project) = run(true, false).await;
        let effects = SideEffects::from_summary(&batches.unwrap());
        assert_eq!(
            (effects.nodes_deleted, effects.relationships_deleted),
            (1, 1)
        );
        assert_eq!(persisted_node_ids(project.path()).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn query_planner_dispatches_every_write_extension_to_its_physical_exec() {
        let dir = TempDir::new().unwrap();
        let (input, _) = empty_write_input();
        let logical_nodes: Vec<(Arc<dyn UserDefinedLogicalNode>, &str)> = vec![
            (
                Arc::new(GraphCreateNode::new(
                    input.clone(),
                    vec![],
                    vec![],
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                )),
                "GraphCreateExec",
            ),
            (
                Arc::new(GraphDeleteNode::new(
                    input.clone(),
                    vec![],
                    false,
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                )),
                "GraphDeleteExec",
            ),
            (
                Arc::new(GraphSetNode::new(
                    input.clone(),
                    vec![],
                    HashMap::new(),
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                )),
                "GraphSetExec",
            ),
            (
                Arc::new(GraphRemoveNode::new(
                    input,
                    vec![],
                    HashMap::new(),
                    dir.path().to_path_buf(),
                    OntologyMode::Exploratory,
                )),
                "GraphRemoveExec",
            ),
        ];
        let context = SessionContext::new();
        let state = context.state();
        let planner = GraphForgeQueryPlanner;

        for (node, expected_name) in logical_nodes {
            let logical = LogicalPlan::Extension(Extension { node });
            let physical = planner
                .create_physical_plan(&logical, &state)
                .await
                .unwrap();
            assert_eq!(physical.name(), expected_name);
        }
    }

    #[tokio::test]
    async fn extension_planner_rejects_missing_write_inputs_and_declines_unknown_nodes() {
        let dir = TempDir::new().unwrap();
        let (input, _) = empty_write_input();
        let nodes: Vec<Arc<dyn UserDefinedLogicalNode>> = vec![
            Arc::new(GraphCreateNode::new(
                input.clone(),
                vec![],
                vec![],
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            )),
            Arc::new(GraphDeleteNode::new(
                input.clone(),
                vec![],
                false,
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            )),
            Arc::new(GraphSetNode::new(
                input.clone(),
                vec![],
                HashMap::new(),
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            )),
            Arc::new(GraphRemoveNode::new(
                input,
                vec![],
                HashMap::new(),
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            )),
        ];
        let state = SessionContext::new().state();
        let physical_planner = DefaultPhysicalPlanner::default();

        for node in nodes {
            let error = GraphForgeExtensionPlanner
                .plan_extension(&physical_planner, node.as_ref(), &[], &[], &state)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("requires one physical input"));
        }

        let unknown = graphforge_plan::GraphMergeNode::new();
        assert!(
            GraphForgeExtensionPlanner
                .plan_extension(&physical_planner, &unknown, &[], &[], &state)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn extension_planner_builds_scan_backed_expand_variants_and_optional_join() {
        let dir = TempDir::new().unwrap();
        let (input, physical) = empty_write_input();
        let expand = graphforge_plan::ExpandNode::new(
            input.clone(),
            "KNOWS",
            1,
            2,
            3,
            graphforge_ir::Direction::Out,
            None,
            dir.path().to_path_buf(),
            OntologyMode::Exploratory,
            vec![],
            vec![],
            vec![],
        );
        let var_len = VarLenExpandNode::new(
            input.clone(),
            "KNOWS",
            1,
            Some(2),
            1,
            2,
            3,
            graphforge_ir::Direction::Out,
            None,
            dir.path().to_path_buf(),
            OntologyMode::Exploratory,
            vec![],
            graphforge_plan::var_len_edge_list_field(&[]),
        );
        let optional = OptionalMatchNode::new(input.clone(), input, vec![], vec![]);
        let state = SessionContext::new().state();
        let planner = DefaultPhysicalPlanner::default();

        let physical_expand = GraphForgeExtensionPlanner
            .plan_extension(
                &planner,
                &expand,
                &[],
                std::slice::from_ref(&physical),
                &state,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(physical_expand.name(), "ExpandExec");
        let physical_var_len = GraphForgeExtensionPlanner
            .plan_extension(
                &planner,
                &var_len,
                &[],
                std::slice::from_ref(&physical),
                &state,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(physical_var_len.name(), "VarLenExpandExec");
        let physical_optional = GraphForgeExtensionPlanner
            .plan_extension(
                &planner,
                &optional,
                &[],
                &[physical.clone(), physical.clone()],
                &state,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(physical_optional.name(), "OptionalMatchExec");

        for node in [
            &expand as &dyn UserDefinedLogicalNode,
            &var_len as &dyn UserDefinedLogicalNode,
        ] {
            assert!(
                GraphForgeExtensionPlanner
                    .plan_extension(&planner, node, &[], &[], &state)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("requires one physical input")
            );
        }
        assert!(
            GraphForgeExtensionPlanner
                .plan_extension(&planner, &optional, &[], &[physical], &state)
                .await
                .unwrap_err()
                .to_string()
                .contains("requires two physical inputs")
        );
    }

    #[tokio::test]
    async fn wave11_physical_graph_execs_reject_missing_children_and_invalid_partitions() {
        use arrow::datatypes::Field;
        use datafusion::logical_expr::col;

        let dir = TempDir::new().unwrap();
        let (input, physical) = empty_write_input();
        let expand = graphforge_plan::ExpandNode::new(
            input.clone(),
            "KNOWS",
            1,
            2,
            3,
            graphforge_ir::Direction::Out,
            None,
            dir.path().to_path_buf(),
            OntologyMode::Exploratory,
            vec![],
            vec![],
            vec![],
        );
        let var_len = VarLenExpandNode::new(
            input.clone(),
            "KNOWS",
            1,
            Some(2),
            1,
            2,
            3,
            graphforge_ir::Direction::Out,
            None,
            dir.path().to_path_buf(),
            OntologyMode::Exploratory,
            vec![],
            graphforge_plan::var_len_edge_list_field(&[]),
        );
        let optional = OptionalMatchNode::new(input.clone(), input.clone(), vec![], vec![]);
        let unwind = graphforge_plan::UnwindNode::new(
            input.clone(),
            col("missing_list"),
            "item",
            &Field::new("item", DataType::Int64, true),
        );
        let infer = graphforge_plan::OntologyInferNode::new(
            input,
            "KNOWS",
            "transitive:KNOWS",
            "conservative_min",
        );
        let state = SessionContext::new().state();
        let planner = DefaultPhysicalPlanner::default();
        let extension = GraphForgeExtensionPlanner;

        let mut one_child = Vec::<Arc<dyn ExecutionPlan>>::new();
        for node in [
            &expand as &dyn UserDefinedLogicalNode,
            &var_len as &dyn UserDefinedLogicalNode,
            &unwind as &dyn UserDefinedLogicalNode,
            &infer as &dyn UserDefinedLogicalNode,
        ] {
            one_child.push(
                extension
                    .plan_extension(&planner, node, &[], std::slice::from_ref(&physical), &state)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let optional = extension
            .plan_extension(
                &planner,
                &optional,
                &[],
                &[physical.clone(), physical],
                &state,
            )
            .await
            .unwrap()
            .unwrap();

        for plan in &one_child {
            assert!(format!("{plan:?}").contains(plan.name()));
            assert!(
                datafusion::physical_plan::displayable(plan.as_ref())
                    .one_line()
                    .to_string()
                    .contains(plan.name())
            );
            assert!(
                plan.clone()
                    .with_new_children(vec![])
                    .unwrap_err()
                    .to_string()
                    .contains("needs one child")
            );
        }
        for plan in one_child.iter().take(3).chain(std::iter::once(&optional)) {
            let error = match plan.execute(1, SessionContext::new().task_ctx()) {
                Ok(_) => panic!("{} accepted invalid partition", plan.name()),
                Err(error) => error,
            };
            assert!(error.to_string().contains("only has partition 0"));
        }
        assert!(
            optional
                .clone()
                .with_new_children(vec![])
                .unwrap_err()
                .to_string()
                .contains("needs two children")
        );
    }

    #[tokio::test]
    async fn wave11_merge_requires_an_explicit_write_target() {
        let session = make_session();
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Merge {
                pattern: graphforge_ir::CreatePattern::default(),
                on_create: vec![],
                on_match: vec![],
            })
            .build();
        let error = session.execute_create(&plan).await.unwrap_err();
        assert!(error.to_string().contains("requires a write target"));
    }

    #[test]
    fn deleted_entity_expression_walkers_cover_every_ir_container() {
        let deleted_var = VarId(7);
        let deleted = HashSet::from([deleted_var]);
        let mut arena = ExprArena::new();
        let var = arena.push(IrExpr::VarRef(deleted_var));
        let literal = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let parameter = arena.push(IrExpr::Parameter("value".into()));
        let property = arena.push(IrExpr::PropertyAccess {
            base: var,
            prop: graphforge_ir::PropId(0),
        });
        let binary = arena.push(IrExpr::BinaryOp {
            op: graphforge_ir::BinaryOpKind::Add,
            left: literal,
            right: property,
        });
        let unary = arena.push(IrExpr::UnaryOp {
            op: graphforge_ir::UnaryOpKind::Neg,
            expr: property,
        });
        let function = arena.push(IrExpr::FunctionCall {
            name: "properties".into(),
            args: vec![var],
        });
        let type_function = arena.push(IrExpr::FunctionCall {
            name: "type".into(),
            args: vec![var],
        });
        let list = arena.push(IrExpr::ListLiteral(vec![literal, property]));
        let map = arena.push(IrExpr::MapLiteral(vec![("key".into(), property)]));
        let case = arena.push(IrExpr::Case {
            operand: Some(literal),
            arms: vec![graphforge_ir::CaseArm {
                when: literal,
                then: var,
            }],
            else_expr: Some(parameter),
        });
        let quantifier = arena.push(IrExpr::Quantifier {
            kind: graphforge_ir::QuantifierKind::Any,
            loop_var: VarId(8),
            list,
            predicate: var,
        });
        let comprehension = arena.push(IrExpr::ListComprehension {
            loop_var: VarId(9),
            list,
            filter: Some(literal),
            projection: Some(var),
        });

        for id in [
            var,
            property,
            binary,
            unary,
            function,
            list,
            map,
            case,
            quantifier,
            comprehension,
        ] {
            assert!(ExecutionSession::expr_references_vars(&arena, id, &deleted));
        }
        assert!(!ExecutionSession::expr_references_vars(
            &arena, parameter, &deleted
        ));
        assert!(ExecutionSession::expr_accesses_deleted_vars(
            &arena, property, &deleted
        ));
        assert!(ExecutionSession::expr_accesses_deleted_vars(
            &arena, function, &deleted
        ));
        assert!(ExecutionSession::expr_accesses_deleted_vars(
            &arena, binary, &deleted
        ));
        assert!(ExecutionSession::expr_accesses_deleted_vars(
            &arena, unary, &deleted
        ));
        assert!(ExecutionSession::expr_accesses_deleted_vars(
            &arena, list, &deleted
        ));
        assert!(ExecutionSession::expr_accesses_deleted_vars(
            &arena, map, &deleted
        ));
        assert!(!ExecutionSession::expr_accesses_deleted_vars(
            &arena,
            type_function,
            &deleted
        ));
    }

    #[test]
    fn execution_session_constructs_without_panic() {
        let _session = make_session();
    }

    #[tokio::test]
    async fn execute_plan_empty_plan_yields_unit_row() {
        // A plan with no source op lowers over the single "unit" row of
        // relational algebra (so `RETURN 1` / `UNWIND [..]` produce output);
        // an entirely empty plan therefore executes to one zero-column row.
        let session = make_session();
        let plan = GraphPlan::builder("openCypher").build();
        let result = session.execute_plan(&plan).await.expect("execute_plan");
        assert_eq!(result.stats.rows_produced, 1);
    }

    #[tokio::test]
    async fn row_count_expressions_resolve_inside_union_inputs() {
        let session = make_session();
        let mut child = GraphPlan::builder("openCypher");
        let one = child.push_expr(IrExpr::Literal(graphforge_ir::IrLiteral::Int(1)));
        let two = child.push_expr(IrExpr::BinaryOp {
            op: graphforge_ir::BinaryOpKind::Add,
            left: one,
            right: one,
        });
        child.push_op_mut(GraphOp::SkipExpr { expr: two });
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Union {
                all: true,
                inputs: vec![child.build()],
            })
            .build();

        let resolved = session
            .resolve_row_count_expressions(&plan, &HashMap::new())
            .await
            .expect("resolve nested row-count expression");
        let [GraphOp::Union { inputs, .. }] = resolved.ops.as_slice() else {
            panic!("expected union");
        };
        assert!(matches!(
            inputs[0].ops.as_slice(),
            [GraphOp::Skip { count: 2 }]
        ));
    }

    #[tokio::test]
    async fn public_write_wrappers_preserve_no_target_and_empty_plan_errors() {
        let session = make_session();
        let plan = GraphPlan::builder("openCypher").build();
        let params = HashMap::from([("value".to_owned(), graphforge_ir::IrLiteral::Int(1))]);

        for result in [
            session.execute_delete(&plan).await,
            session.execute_delete_with_params(&plan, &params).await,
            session.execute_set(&plan).await,
            session.execute_set_with_params(&plan, &params).await,
            session.execute_remove(&plan).await,
            session.execute_remove_with_params(&plan, &params).await,
        ] {
            let error = result.expect_err("write wrappers require a write target");
            assert!(
                error.to_string().contains("write target"),
                "unexpected error: {error}"
            );
        }
    }

    #[tokio::test]
    async fn public_create_set_remove_params_persist_across_session_reopen() {
        use graphforge_ir::{
            CreateNodeSpec, CreatePattern, LabelItem, RemovePropItem, SetMapItem, SetPropItem,
        };

        let dir = TempDir::new().unwrap();
        let open_session = || {
            let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
            ExecutionSession::new_with_target(
                catalog,
                None,
                dir.path().to_path_buf(),
                OntologyMode::Exploratory,
            )
            .unwrap()
        };

        let create = GraphPlan::builder("openCypher")
            .push_op(GraphOp::Create {
                pattern: CreatePattern {
                    nodes: vec![CreateNodeSpec {
                        var: VarId(0),
                        labels: vec![],
                        properties: None,
                        is_reference: false,
                    }],
                    edges: vec![],
                },
            })
            .build();
        let session = open_session();
        let created = session.execute_create(&create).await.unwrap();
        assert_eq!(created.side_effects.unwrap().nodes_created, 1);
        drop(session);

        let mut map_set = GraphPlan::builder("openCypher");
        let name = map_set.push_expr(IrExpr::Literal(IrLiteral::Str("Ada".into())));
        let active = map_set.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
        let map = map_set.push_expr(IrExpr::MapLiteral(vec![
            ("name".into(), name),
            ("active".into(), active),
        ]));
        let map_set = map_set
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Set {
                items: vec![],
                map_items: vec![SetMapItem {
                    target: VarId(0),
                    map,
                    replace: false,
                }],
                label_items: vec![LabelItem {
                    target: VarId(0),
                    labels: vec![graphforge_core::TypeId(7)],
                }],
            })
            .build();
        let session = open_session();
        let map_result = session.execute_set(&map_set).await.unwrap();
        let map_effects = map_result.side_effects.unwrap();
        assert_eq!(map_effects.properties_set, 2);
        assert_eq!(map_effects.labels_added, 1);
        drop(session);

        let mut set = GraphPlan::builder("openCypher");
        let score = set.push_expr(IrExpr::Parameter("score".into()));
        let set = set
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Set {
                items: vec![SetPropItem {
                    target: VarId(0),
                    prop: graphforge_core::PropId(0),
                    prop_name: "score".into(),
                    value: score,
                }],
                map_items: vec![],
                label_items: vec![],
            })
            .build();
        let session = open_session();
        let set_result = session
            .execute_set_with_params(&set, &HashMap::from([("score".into(), IrLiteral::Int(42))]))
            .await
            .unwrap();
        assert_eq!(set_result.side_effects.unwrap().properties_set, 1);
        drop(session);

        let remove = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::Remove {
                items: vec![RemovePropItem {
                    target: VarId(0),
                    prop: graphforge_core::PropId(0),
                    prop_name: "score".into(),
                }],
                label_items: vec![LabelItem {
                    target: VarId(0),
                    labels: vec![graphforge_core::TypeId(7)],
                }],
            })
            .build();
        let session = open_session();
        let removed = session.execute_remove(&remove).await.unwrap();
        let removed = removed.side_effects.unwrap();
        assert_eq!(removed.properties_removed, 1);
        assert_eq!(removed.labels_removed, 1);
    }

    #[test]
    fn context_exposes_graph_catalog() {
        let session = make_session();
        assert!(
            session.context().catalog("graph").is_some(),
            "graph catalog should be registered"
        );
    }

    #[test]
    fn graph_schema_has_topology_nodes_table() {
        let session = make_session();
        let catalog = session.context().catalog("graph").unwrap();
        let schema = catalog.schema("graph").unwrap();
        assert!(
            schema.table_exist("topology_nodes"),
            "topology_nodes table should be registered"
        );
    }
}
