//! Traversal execution and generation-pinned ordinal identity.

use crate::ValueAt;
use crate::adjacency;
use crate::adjacency::AdjacencyProvider;
use crate::demand;
use crate::read_resource;
use crate::string_column;
use crate::to_df_err;
use crate::u64_column;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::ListBuilder;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt32Array;
use arrow::array::UInt32Builder;
use arrow::array::UInt64Array;
use arrow::array::new_null_array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use edge_filter::PhysicalEdgeFilter;
pub(crate) use edge_properties::EdgePropSource;
use edge_properties::build_edge_prop_children;
use edge_properties::edge_prop_children;
use edge_properties::load_edge_prop_source;
use futures::StreamExt;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_ir::Direction;
use graphforge_plan::VarLenExpandNode;
use graphforge_plan::VarLenMode;
use reachability::reach_bfs;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

// ---------------------------------------------------------------------------
// Partitioning shared by the traversal operators
// ---------------------------------------------------------------------------

/// Plan properties of a traversal operator: partition `i` of the output is
/// computed from partition `i` of `input`, so the operator exposes exactly the
/// input's partition count and no ordering.
///
/// Every input row is expanded independently of every other, so the rows of
/// the output are the same multiset however the input is split. A traversal
/// therefore runs on as many tasks as the planner gave its input (#1984).
///
/// A round-robin input stays round-robin: that partitioning promises nothing
/// about which rows share a partition, so it survives any per-row expansion,
/// and the storage approval rules that recognise a round-robin frontier keep
/// matching the plan they matched when an exchange sat above the traversal.
/// A hash input would no longer hash the expanded columns, so it is reported
/// as unknown.
fn partitioned_properties(
    schema: &SchemaRef,
    input: &Arc<dyn ExecutionPlan>,
) -> Arc<PlanProperties> {
    let partitioning = match input.output_partitioning() {
        Partitioning::RoundRobinBatch(partitions) => Partitioning::RoundRobinBatch(*partitions),
        other => Partitioning::UnknownPartitioning(other.partition_count()),
    };
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(Arc::clone(schema)),
        partitioning,
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

/// Reject a partition number the input cannot serve.
fn check_partition(
    operator: &str,
    input: &Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Result<(), DataFusionError> {
    let partitions = input.output_partitioning().partition_count();
    if partition >= partitions {
        return Err(DataFusionError::Internal(format!(
            "{operator} has {partitions} partitions, got partition {partition}"
        )));
    }
    Ok(())
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
    /// Column index of an already-bound destination node's `node_id` within the
    /// input: only paths reaching that node are materialised (#1986).
    bound_dst_col_idx: Option<usize>,
    /// Whether rows are trails or reached nodes; see [`VarLenMode`].
    expansion: VarLenMode,
    /// The edge variable, the key of this hop's demand counters.
    edge_var: u32,
    /// Whether any operator above reads the trailing edge-list column. When
    /// none does, the traversal still produces one row per trail but reads no
    /// edge file: the trails come from the adjacency's edge ids alone and the
    /// column holds nulls.
    edge_list_demanded: bool,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    /// Adjacency source for the BFS — the session-scoped provider injected
    /// by the extension planner (#761).
    provider: Arc<dyn AdjacencyProvider>,
    capture_epoch: u64,
    /// The `ALL(e IN r WHERE ..)` predicate each crossed edge must satisfy;
    /// only reachability expansions carry one (#1984).
    edge_filter: Option<Arc<PhysicalEdgeFilter>>,
}

impl VarLenExpandExec {
    /// Build the physical node from its logical counterpart, planned input,
    /// and the session's adjacency provider (#761).
    #[must_use]
    pub fn new(
        node: &VarLenExpandNode,
        input: Arc<dyn ExecutionPlan>,
        provider: Arc<dyn AdjacencyProvider>,
        dir: PathBuf,
        mode: OntologyMode,
    ) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = partitioned_properties(&schema, &input);
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
        let bound_dst_col_idx = node.bound_destination.as_ref().and_then(|alias| {
            node.input.schema().index_of_column_by_name(
                Some(&datafusion::common::TableReference::bare(alias.as_str())),
                "node_id",
            )
        });
        Self {
            input,
            rel_type_name: node.rel_type_name.clone(),
            direction: node.direction,
            min_hops: node.min_hops,
            max_hops: node.max_hops,
            dir,
            mode,
            src_col_idx,
            bound_dst_col_idx,
            expansion: node.mode,
            edge_var: node.edge_var,
            edge_list_demanded: true,
            schema,
            props,
            provider,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
            edge_filter: None,
        }
    }

    /// Declare whether any operator above reads the edge-list column.
    ///
    /// Only a trail expansion has one; a reachability expansion produces hop
    /// counts and keeps its plan unchanged.
    pub(super) fn with_edge_list_demanded(&self, demanded: bool) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            bound_dst_col_idx: self.bound_dst_col_idx,
            expansion: self.expansion,
            edge_var: self.edge_var,
            edge_list_demanded: demanded || self.expansion != VarLenMode::Trails,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            capture_epoch: self.capture_epoch,
            edge_filter: self.edge_filter.clone(),
        })
    }

    /// Require every edge the expansion crosses to satisfy `predicate`, a
    /// planned expression over one struct made of `fields` (see
    /// [`graphforge_plan::EdgeFilter`]).
    #[must_use]
    pub fn with_edge_filter(
        mut self,
        predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        fields: &[arrow::datatypes::FieldRef],
    ) -> Self {
        self.edge_filter = Some(Arc::new(PhysicalEdgeFilter::new(predicate, fields)));
        self
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
                .explain_status(&self.rel_type_name, self.direction)
        )?;
        if self.expansion == VarLenMode::Reachability {
            f.write_str(", mode=reachability")?;
        }
        if let Some(filter) = &self.edge_filter {
            write!(f, ", edge_filter={}", filter.predicate())?;
        }
        Ok(())
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
        let props = partitioned_properties(&self.schema, &input);
        Ok(Arc::new(Self {
            input,
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            bound_dst_col_idx: self.bound_dst_col_idx,
            expansion: self.expansion,
            edge_var: self.edge_var,
            edge_list_demanded: self.edge_list_demanded,
            schema: self.schema.clone(),
            props,
            provider: self.provider.clone(),
            capture_epoch: self.capture_epoch,
            edge_filter: self.edge_filter.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        check_partition("VarLenExpandExec", &self.input, partition)?;
        // The traversal runs as a task that consumes the input as it arrives and
        // sends bounded batches through a small channel, so the paths of every
        // seed never exist at once (#1986).
        let input = self.input.clone();
        let cfg = ExpandConfig {
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            bound_dst_col_idx: self.bound_dst_col_idx,
            expansion: self.expansion,
            edge_var: self.edge_var,
            edge_list_demanded: self.edge_list_demanded,
            out_schema: self.schema.clone(),
            provider: self.provider.clone(),
            capture_epoch: self.capture_epoch,
            edge_filter: self.edge_filter.clone(),
            lifecycle: graphforge_storage::lifecycle_io::CaptureContext::current(),
        };
        let mut reservation = MemoryConsumer::new(VAR_LEN_CONSUMER)
            .with_can_spill(false)
            .register(context.memory_pool());
        let mut builder = RecordBatchReceiverStream::builder(self.schema.clone(), 2);
        let tx = builder.tx();
        builder.spawn(async move {
            let input = input.execute(partition, context)?;
            expand_bfs(&cfg, input, &tx, &mut reservation)
                .await
                .map_err(to_df_err)
        });
        Ok(builder.build())
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
        let props = partitioned_properties(&schema, &input);
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
        let props = partitioned_properties(&self.schema, &input);
        Ok(Arc::new(Self {
            input,
            rule_id: self.rule_id.clone(),
            confidence_model: self.confidence_model.clone(),
            schema: self.schema.clone(),
            props,
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
pub(super) struct ExpandConfig {
    pub(super) rel_type_name: String,
    pub(super) direction: Direction,
    pub(super) min_hops: u16,
    pub(super) max_hops: Option<u16>,
    pub(super) dir: PathBuf,
    pub(super) mode: OntologyMode,
    /// Column index of the BFS seed within the input batch (the source
    /// variable's `node_id`); resolved by qualifier at construction time.
    pub(super) src_col_idx: usize,
    /// Input column of an already-bound destination's `node_id`; paths that do
    /// not end there are dropped before they are materialised (#1986).
    pub(super) bound_dst_col_idx: Option<usize>,
    /// Whether rows are trails or reached nodes; see [`VarLenMode`].
    pub(super) expansion: VarLenMode,
    /// The edge variable, the key of this hop's demand counters.
    pub(super) edge_var: u32,
    /// Whether the trailing edge-list column is read above this expansion; see
    /// [`VarLenExpandExec`]. Always true for a reachability expansion.
    pub(super) edge_list_demanded: bool,
    pub(super) out_schema: SchemaRef,
    /// Adjacency source (#762) — moved into the `'static` execute future.
    pub(super) provider: Arc<dyn AdjacencyProvider>,
    pub(super) capture_epoch: u64,
    /// The predicate every crossed edge must satisfy (reachability only).
    pub(super) edge_filter: Option<Arc<PhysicalEdgeFilter>>,
    /// The statement's read-path observer. The traversal runs as a spawned
    /// task, so each synchronous section attaches it to count its reads.
    pub(super) lifecycle: graphforge_storage::lifecycle_io::CaptureContext,
}

/// One in-progress path during the variable-length BFS.
struct PathState {
    /// Current node (the path's frontier).
    node: u64,
    /// Edge ids in traversal order — the relationship list bound to the edge
    /// var (#709). `edge_path.len() == hops` by construction; it doubles as the
    /// relationship-isomorphism set (a path is at most `max_hops` long).
    edge_path: Vec<u64>,
    /// Hops taken so far.
    hops: u16,
    /// Index of the originating source row (to carry its input columns).
    input_row: usize,
}

/// Output-batch bounds for the variable-length traversal (#1986). A batch holds
/// at most [`EXPAND_BATCH_MAX_ROWS`] paths and [`EXPAND_BATCH_MAX_HOPS`]
/// relationship entries, and its copied input columns stay near
/// [`EXPAND_BATCH_INPUT_BYTES`], so no Arrow offset buffer in one batch can reach
/// 2 GiB however many paths the query admits.
const EXPAND_BATCH_MAX_ROWS: usize = 65_536;
const EXPAND_BATCH_MAX_HOPS: usize = 1 << 22;
const EXPAND_BATCH_INPUT_BYTES: usize = 64 << 20;

/// One emitted path: `(input_row, reached_node_id, edge_ids_in_order)`.
type Emission = (usize, u64, Vec<u64>);

/// The memory-pool consumer a variable-length expansion charges the state it
/// holds between batches to. The reservation cannot spill: when the pool refuses
/// a charge the query fails naming this consumer.
///
/// Charged while held, released as soon as it is handed on or dropped:
/// - the input batch being expanded: `get_array_memory_size()`;
/// - a queued path: [`path_state_bytes`], released when it is expanded;
/// - an emitted path: [`emission_bytes`], released when the batch carrying it is
///   sent;
/// - while a batch is built: the traversed-edge and reached-node sets
///   ([`hash_table_bytes`]), the edge batches and records
///   ([`edge_record_table_bytes`]), the destination node batch and its row map,
///   all released when the batch is built;
/// - the output batch: `get_array_memory_size()`, released when it is sent.
///
/// So the reservation tracks live state: one input batch, one seed's queue and
/// paths, and one output batch, never the whole result.
const VAR_LEN_CONSUMER: &str = "VarLenExpand path state";

/// Heap bytes of a hash table with `capacity` usable slots of `entry_bytes`
/// each: one control byte per bucket and `8/7` buckets per usable slot
/// (hashbrown's maximum load factor), plus `16` bytes of control-group padding.
fn hash_table_bytes(capacity: usize, entry_bytes: usize) -> usize {
    capacity
        .saturating_mul(entry_bytes + 1)
        .saturating_mul(8)
        .div_ceil(7)
        .saturating_add(16)
}

/// Charged bytes of one queued path: `2 * size_of::<PathState>()` (its
/// `VecDeque` slot plus up to one slot of doubling slack) `+ 8 * edge_path.capacity()`.
fn path_state_bytes(state: &PathState) -> usize {
    (2 * std::mem::size_of::<PathState>()).saturating_add(8 * state.edge_path.capacity())
}

/// Charged bytes of one emitted path: `2 * size_of::<Emission>()` (its `Vec`
/// slot plus doubling slack) `+ 8 * hops` for its edge ids.
fn emission_bytes(hops: usize) -> usize {
    (2 * std::mem::size_of::<Emission>()).saturating_add(8 * hops)
}

/// Charged bytes of the edge-record table: the table at its capacity plus each
/// record's relation-type string.
fn edge_record_table_bytes(records: &std::collections::HashMap<u64, EdgeRecord>) -> usize {
    records.values().fold(
        hash_table_bytes(records.capacity(), std::mem::size_of::<(u64, EdgeRecord)>()),
        |bytes, record| bytes.saturating_add(record.rel_type.as_ref().map_or(0, String::capacity)),
    )
}

fn grow_reservation(reservation: &mut MemoryReservation, bytes: usize) -> Result<(), GfError> {
    reservation
        .try_grow(bytes)
        .map_err(GfError::from_execution_error)
}

/// Send one built batch and release its charge: the consumer owns it now.
/// `false` once the consumer is gone.
async fn send_chunk(
    cfg: &ExpandConfig,
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    out: RecordBatch,
    reservation: &mut MemoryReservation,
) -> bool {
    let bytes = out.get_array_memory_size();
    if cfg.capture_epoch != 0 {
        demand::record_emitted(cfg.capture_epoch, cfg.edge_var, out.num_rows());
    }
    let sent = tx.send(Ok(out)).await.is_ok();
    reservation.shrink(bytes);
    sent
}

/// Rows of an output batch that copies one row of `input` per emitted row. Every
/// emitted row copies one input row, so the rows per batch follow the input's
/// average row width. Width is the bytes the rows occupy, not the capacity of the
/// buffers they were sliced from: one row cut from an 8,192-row scan batch
/// reports the whole batch's buffers and would cap every output batch at a
/// handful of rows.
fn output_batch_rows(input: &RecordBatch) -> usize {
    let input_slice_bytes = input
        .columns()
        .iter()
        .map(|column| {
            column
                .to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| column.get_array_memory_size())
        })
        .sum::<usize>();
    let row_bytes = input_slice_bytes
        .checked_div(input.num_rows())
        .unwrap_or(0)
        .max(1);
    (EXPAND_BATCH_INPUT_BYTES / row_bytes).clamp(1, EXPAND_BATCH_MAX_ROWS)
}

/// Run the variable-length BFS and send the output as bounded batches.
///
/// Output columns = the input (source) columns, then the destination node's
/// `TOPOLOGY_NODES_SCHEMA` columns, then a trailing `List<Struct>` edge-list
/// column (the relationship list bound to the edge var, #709) — for every path
/// whose hop count lands in `[min_hops, max_hops]`. Input batches are consumed
/// as they arrive and seeds are expanded in input order; each output batch is
/// built and sent before the next seed is expanded, so memory is bounded by one
/// input batch, one output batch and one seed's paths. Stops quietly once the
/// consumer is gone.
///
/// What is held between batches is charged to `reservation` (see
/// [`VAR_LEN_CONSUMER`]); the pool refusing a charge fails the query.
#[allow(
    clippy::too_many_lines,
    reason = "one loop owns the input batches, the seeds and every charge made for what they hold"
)]
async fn expand_bfs(
    cfg: &ExpandConfig,
    mut input: SendableRecordBatchStream,
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    reservation: &mut MemoryReservation,
) -> Result<(), GfError> {
    if cfg.expansion == VarLenMode::Reachability {
        return reach_bfs(cfg, input, tx, reservation).await;
    }
    let exec_err = |m: String| GfError::Execution(m);

    // --- Obtain the directed adjacency the traversal needs (#762). ---
    let mut adjacency = {
        let _capture = cfg.lifecycle.attach();
        adjacency::AdjacencyReader::for_capture(
            cfg.provider.as_ref(),
            &cfg.rel_type_name,
            cfg.direction,
            cfg.capture_epoch,
        )?
    };

    let mut sent_any = false;
    while let Some(batch) = input.next().await {
        let input = batch.map_err(GfError::from_execution_error)?;
        let input_bytes = input.get_array_memory_size();
        grow_reservation(reservation, input_bytes)?;
        if cfg.capture_epoch != 0 {
            demand::record_input(cfg.capture_epoch, cfg.edge_var, input.num_rows());
        }
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
        let bound_dst = cfg
            .bound_dst_col_idx
            .map(|index| u64_column(&input, index))
            .transpose()?;

        let max_rows = output_batch_rows(&input);

        let mut pending: Vec<Emission> = Vec::new();
        let mut pending_hops = 0usize;
        let mut pending_bytes = 0usize;
        for (row, seed) in src_ids.iter().enumerate() {
            let Some(node) = seed else { continue };
            // The query equates the destination with an input node: a NULL
            // there matches nothing, otherwise only paths ending at it count.
            let required_dst = match &bound_dst {
                None => None,
                Some(ids) if ids.is_null(row) => continue,
                Some(ids) => Some(ids.value(row)),
            };
            let seed_emissions =
                bfs_emit_seed(cfg, &mut adjacency, row, node, required_dst, reservation)?;
            let seed_hops = seed_emissions.iter().fold(0usize, |hops, emission| {
                hops.saturating_add(emission.2.len())
            });
            if seed_emissions.len() >= max_rows || seed_hops >= EXPAND_BATCH_MAX_HOPS {
                // A seed whose paths fill several output batches shares the
                // edges, destination nodes and edge properties of those paths
                // between them: it reads them once and slices the result, not
                // once per output batch. Its batches never mix with other seeds'.
                if !pending.is_empty() {
                    sent_any = true;
                    let out = expand_chunk(cfg, &input, &pending, reservation)?;
                    pending.clear();
                    reservation.shrink(pending_bytes);
                    pending_hops = 0;
                    pending_bytes = 0;
                    if !send_chunk(cfg, tx, out, reservation).await {
                        return Ok(());
                    }
                }
                if !emit_large_seed(
                    cfg,
                    &input,
                    &seed_emissions,
                    max_rows,
                    tx,
                    reservation,
                    &mut sent_any,
                )
                .await?
                {
                    return Ok(());
                }
                continue;
            }
            for emission in seed_emissions {
                pending_hops = pending_hops.saturating_add(emission.2.len());
                pending_bytes = pending_bytes.saturating_add(emission_bytes(emission.2.len()));
                pending.push(emission);
                if pending.len() >= max_rows || pending_hops >= EXPAND_BATCH_MAX_HOPS {
                    let out = expand_chunk(cfg, &input, &pending, reservation)?;
                    pending.clear();
                    reservation.shrink(pending_bytes);
                    pending_hops = 0;
                    pending_bytes = 0;
                    sent_any = true;
                    if !send_chunk(cfg, tx, out, reservation).await {
                        return Ok(());
                    }
                }
            }
        }
        if !pending.is_empty() {
            sent_any = true;
            let out = expand_chunk(cfg, &input, &pending, reservation)?;
            reservation.shrink(pending_bytes);
            if !send_chunk(cfg, tx, out, reservation).await {
                return Ok(());
            }
        }
        reservation.shrink(input_bytes);
    }
    // No matched paths → one empty output under the planned schema. A closed
    // receiver only means the consumer is gone.
    if !sent_any {
        let _ = tx
            .send(Ok(RecordBatch::new_empty(cfg.out_schema.clone())))
            .await;
    }
    Ok(())
}

/// Build and send the output batches of a seed whose paths fill several of
/// them. Its edges, destination nodes and edge properties are read once for the
/// whole seed and sliced into batches of at most `max_rows` paths and
/// [`EXPAND_BATCH_MAX_HOPS`] hops. `false` once the consumer is gone.
async fn emit_large_seed(
    cfg: &ExpandConfig,
    input: &RecordBatch,
    seed_emissions: &[Emission],
    max_rows: usize,
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    reservation: &mut MemoryReservation,
    sent_any: &mut bool,
) -> Result<bool, GfError> {
    let seed_bytes = seed_emissions.iter().fold(0usize, |bytes, emission| {
        bytes.saturating_add(emission_bytes(emission.2.len()))
    });
    if let Some(sources) = load_chunk_sources(cfg, seed_emissions, reservation)? {
        let mut start = 0usize;
        while start < seed_emissions.len() {
            let mut end = start;
            let mut hops = 0usize;
            while end < seed_emissions.len()
                && end - start < max_rows
                && hops < EXPAND_BATCH_MAX_HOPS
            {
                hops = hops.saturating_add(seed_emissions[end].2.len());
                end += 1;
            }
            let out = materialise_chunk(
                cfg,
                input,
                &seed_emissions[start..end],
                &sources,
                reservation,
            )?;
            *sent_any = true;
            if !send_chunk(cfg, tx, out, reservation).await {
                return Ok(false);
            }
            start = end;
        }
        sources.release(reservation);
    }
    reservation.shrink(seed_bytes);
    Ok(true)
}

/// What materialising a set of emissions reads from storage: the identity of
/// every traversed edge, the reached destination node records and the relation's
/// edge properties. Loaded once for the emissions it covers, so a seed whose
/// paths fill several output batches reads each of them once, not once per batch.
///
/// What it holds is charged to the reservation; [`ChunkSources::release`] gives
/// the charge back once the last batch built from it has been built.
struct ChunkSources {
    edge_records: std::collections::HashMap<u64, EdgeRecord>,
    node_batch: RecordBatch,
    node_row: std::collections::HashMap<u64, usize>,
    edge_props: EdgePropSource,
    charged: usize,
}

impl ChunkSources {
    fn release(self, reservation: &mut MemoryReservation) {
        reservation.shrink(self.charged);
    }
}

/// Charge `bytes` and add them to the running total of `charged`.
fn charge(
    reservation: &mut MemoryReservation,
    charged: &mut usize,
    bytes: usize,
) -> Result<(), GfError> {
    grow_reservation(reservation, bytes)?;
    *charged = charged.saturating_add(bytes);
    Ok(())
}

/// The struct fields of the edge-list column, past the four topology fields:
/// the relation's persisted edge properties (#755).
fn edge_list_prop_fields(cfg: &ExpandConfig) -> Result<Vec<arrow::datatypes::FieldRef>, GfError> {
    use arrow::datatypes::DataType;

    let exec_err = |m: String| GfError::Execution(m);
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
    Ok(struct_fields.iter().skip(4).cloned().collect())
}

/// Read everything `emissions` need from storage. `None` when there is no node
/// batch to take destinations from, which yields an empty output.
///
/// The traversed and reached sets and the edge batches are charged while they
/// are alive and released before this returns; the edge records, node batch,
/// row map and edge properties stay charged to the returned sources.
#[allow(
    clippy::too_many_lines,
    reason = "one function owns the storage reads and every charge made for what they hold"
)]
fn load_chunk_sources(
    cfg: &ExpandConfig,
    emissions: &[Emission],
    reservation: &mut MemoryReservation,
) -> Result<Option<ChunkSources>, GfError> {
    use std::collections::HashMap;

    use arrow::compute::concat_batches;

    let exec_err = |m: String| GfError::Execution(m);
    let _capture = cfg.lifecycle.attach();
    // Charged for the whole function, released before it returns.
    let mut temporary = 0usize;
    // Charged to the returned sources.
    let mut charged = 0usize;

    // Nothing is read from an edge file when no operator above reads the
    // relationship list: the trails are already complete from the adjacency's
    // edge ids, so there is nothing to look up.
    let traversed: std::collections::HashSet<u64> = if cfg.edge_list_demanded {
        emissions
            .iter()
            .flat_map(|(_, _, path)| path.iter().copied())
            .collect()
    } else {
        std::collections::HashSet::new()
    };
    charge(
        reservation,
        &mut temporary,
        hash_table_bytes(traversed.capacity(), 8),
    )?;

    // Edge records (public identity + rel type) keyed by edge_id, for assembling
    // the per-path relationship list (#709) - read lazily for the traversed
    // ids only (row-group pruning + row filter; an empty traversal never
    // opens the file).
    let edge_records = if cfg.edge_list_demanded {
        let edge_batches = read_traversed_edges(cfg, &traversed)?;
        charge(
            reservation,
            &mut temporary,
            edge_batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum(),
        )?;
        let edge_records = build_edge_records(cfg, &edge_batches)?;
        charge(
            reservation,
            &mut charged,
            edge_record_table_bytes(&edge_records),
        )?;
        drop(edge_batches);
        edge_records
    } else {
        HashMap::new()
    };

    // --- Read nodes; map node_id -> row index for destination columns. ---
    // Lazily read only the reached destination node records (#838): the dst
    // columns are projected solely from these node_ids (incl. a 0-hop self
    // row's seed, which is in `emissions`), so an index Hit no longer scans the
    // whole node table. Source columns come from the input batch, not here.
    let reached: std::collections::HashSet<u64> = emissions.iter().map(|(_, id, _)| *id).collect();
    charge(
        reservation,
        &mut temporary,
        hash_table_bytes(reached.capacity(), 8),
    )?;
    let node_batches = graphforge_storage::read_nodes_filtered_from_files(
        &topology_for_provider(cfg.provider.as_ref(), &cfg.dir)?,
        &reached,
    )
    .map_err(|e| exec_err(e.to_string()))?;
    // `read_nodes_filtered` always returns at least one (possibly empty) batch,
    // but guard defensively: with no node batch there is nothing to reach, so
    // emit zero rows rather than indexing into an empty Vec.
    let Some(first) = node_batches.first() else {
        reservation.shrink(temporary.saturating_add(charged));
        return Ok(None);
    };
    let node_batch =
        concat_batches(&first.schema(), &node_batches).map_err(|e| exec_err(e.to_string()))?;
    charge(
        reservation,
        &mut charged,
        node_batch.get_array_memory_size(),
    )?;
    let node_ids = u64_column(&node_batch, 1)?; // node_id is column 1
    let node_row: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .filter_map(|(i, id)| id.map(|v| (v, i)))
        .collect();
    charge(
        reservation,
        &mut charged,
        hash_table_bytes(node_row.capacity(), std::mem::size_of::<(u64, usize)>()),
    )?;

    // The relation's edge properties for every traversed edge.
    let edge_props = if cfg.edge_list_demanded {
        let prop_fields = edge_list_prop_fields(cfg)?;
        let mut hop_uuids = std::collections::BTreeSet::new();
        for id in &traversed {
            let record = edge_records
                .get(id)
                .ok_or_else(|| exec_err(format!("VarLenExpand: no record for edge_id {id}")))?;
            hop_uuids.insert(record.edge_uuid);
        }
        load_edge_prop_source(
            cfg.provider.admitted_inventory().as_deref(),
            &cfg.rel_type_name,
            &cfg.dir,
            &prop_fields,
            &hop_uuids,
        )?
    } else {
        EdgePropSource::empty()
    };
    charge(reservation, &mut charged, edge_props.charged_bytes())?;

    reservation.shrink(temporary);
    Ok(Some(ChunkSources {
        edge_records,
        node_batch,
        node_row,
        edge_props,
        charged,
    }))
}

/// Build one output batch for `emissions` (a bounded slice of the traversal)
/// from sources that cover them. The returned batch is charged until
/// [`send_chunk`] hands it on.
fn materialise_chunk(
    cfg: &ExpandConfig,
    input: &RecordBatch,
    emissions: &[Emission],
    sources: &ChunkSources,
    reservation: &mut MemoryReservation,
) -> Result<RecordBatch, GfError> {
    use arrow::compute::take;

    let exec_err = |m: String| GfError::Execution(m);
    let _capture = cfg.lifecycle.attach();
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
                let row = sources.node_row.get(id).ok_or_else(|| {
                    exec_err(format!(
                        "VarLenExpand reached unknown destination node_id {id}"
                    ))
                })?;
                to_u32(*row)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );

    // --- Materialise the output batch via `take` on input + node columns. ---
    let mut columns =
        Vec::with_capacity(input.num_columns() + sources.node_batch.num_columns() + 1);
    for col in input.columns() {
        columns.push(take(col, &src_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    for col in sources.node_batch.columns() {
        columns.push(take(col, &dst_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    // Trailing edge-list column (#709): one `List<Struct>` value per emitted
    // path, in schema order (last). Built from each path's ordered edge ids,
    // plus the relation's edge properties (#755).
    if cfg.edge_list_demanded {
        columns.push(build_edge_list_column(
            cfg,
            emissions,
            &sources.edge_records,
            &sources.edge_props,
        )?);
    } else {
        // No operator above reads the list, so no entry of it is ever looked
        // up; the column is nullable and carries nulls.
        let edge_field = cfg
            .out_schema
            .fields()
            .last()
            .ok_or_else(|| exec_err("output schema has no edge-list column".into()))?;
        columns.push(new_null_array(edge_field.data_type(), emissions.len()));
    }
    let out = RecordBatch::try_new(cfg.out_schema.clone(), columns)
        .map_err(|e| exec_err(e.to_string()))?;
    grow_reservation(reservation, out.get_array_memory_size())?;
    Ok(out)
}

/// Build one output batch for `emissions`, reading what they need.
///
/// What is read is charged while the batch is built and released before this
/// returns; the returned batch stays charged until [`send_chunk`] hands it on.
fn expand_chunk(
    cfg: &ExpandConfig,
    input: &RecordBatch,
    emissions: &[Emission],
    reservation: &mut MemoryReservation,
) -> Result<RecordBatch, GfError> {
    let Some(sources) = load_chunk_sources(cfg, emissions, reservation)? else {
        let empty = RecordBatch::new_empty(cfg.out_schema.clone());
        grow_reservation(reservation, empty.get_array_memory_size())?;
        return Ok(empty);
    };
    let out = materialise_chunk(cfg, input, emissions, &sources, reservation)?;
    sources.release(reservation);
    Ok(out)
}

/// Edge identity columns of the traversed edges. Only the identity columns (and
/// the relation name when the file carries one) are decoded; `edge_id` keys the
/// records.
fn read_traversed_edges(
    cfg: &ExpandConfig,
    traversed: &std::collections::HashSet<u64>,
) -> Result<Vec<RecordBatch>, GfError> {
    let exec_err = |m: String| GfError::Execution(m);
    let mut edge_projection = vec![0_usize, 1, 2];
    if cfg.mode == OntologyMode::Exploratory || cfg.rel_type_name == "*" {
        edge_projection.push(
            graphforge_storage::EXPLORATORY_EDGE_SCHEMA
                .index_of("rel_type_name")
                .map_err(|e| exec_err(e.to_string()))?,
        );
    }
    match cfg.provider.admitted_inventory() {
        Some(inventory) => graphforge_storage::read_edges_filtered_projected_from_inventory(
            &inventory,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            &edge_projection,
            None,
        ),
        None => graphforge_storage::read_edges_filtered_projected_observed(
            &cfg.dir,
            &cfg.rel_type_name,
            cfg.mode,
            traversed,
            &edge_projection,
            None,
        ),
    }
    .map_err(|e| exec_err(e.to_string()))
}

/// BFS one source seed over `adjacency`, with per-path relationship
/// isomorphism (no edge reused on a path). Returns one emission per matched
/// path: `(input_row, reached_node_id, edge_ids_in_order)`, breadth-first.
///
/// `min_hops == 0` (Cypher `*0..`) emits the 0-hop source-to-self path (empty
/// edge list); extension stops once `max_hops` is reached.
///
/// Every queued path ([`path_state_bytes`]) and emitted path ([`emission_bytes`])
/// is charged to `reservation` when created; a queued path is released once
/// expanded. The returned emissions stay charged: the caller releases them when
/// the batch carrying them is sent.
fn bfs_emit_seed(
    cfg: &ExpandConfig,
    adjacency: &mut adjacency::AdjacencyReader<'_>,
    input_row: usize,
    seed: u64,
    required_dst: Option<u64>,
    reservation: &mut MemoryReservation,
) -> Result<Vec<Emission>, GfError> {
    use std::collections::VecDeque;

    let _capture = cfg.lifecycle.attach();

    let mut emissions: Vec<Emission> = Vec::new();
    let mut queue: VecDeque<PathState> = VecDeque::new();
    let seed_state = PathState {
        node: seed,
        edge_path: Vec::new(),
        hops: 0,
        input_row,
    };
    grow_reservation(reservation, path_state_bytes(&seed_state))?;
    queue.push_back(seed_state);
    // Adjacency entries examined for this seed, recorded once at the end.
    let mut examined = 0_usize;
    while let Some(p) = queue.pop_front() {
        let in_range = p.hops >= cfg.min_hops && cfg.max_hops.is_none_or(|m| p.hops <= m);
        if in_range && required_dst.is_none_or(|dst| dst == p.node) {
            grow_reservation(reservation, emission_bytes(p.edge_path.len()))?;
            emissions.push((p.input_row, p.node, p.edge_path.clone()));
        }
        if cfg.max_hops.is_some_and(|m| p.hops >= m) {
            reservation.shrink(path_state_bytes(&p));
            continue;
        }
        adjacency.with_neighbors(p.node, |neighbors| {
            examined = examined.saturating_add(neighbors.len());
            for (edge_id, next) in neighbors.iter() {
                if p.edge_path.contains(&edge_id) {
                    continue; // relationship isomorphism: no edge twice per path
                }
                let mut edge_path = Vec::with_capacity(p.edge_path.len() + 1);
                edge_path.extend_from_slice(&p.edge_path);
                edge_path.push(edge_id);
                let state = PathState {
                    node: next,
                    edge_path,
                    hops: p.hops + 1,
                    input_row: p.input_row,
                };
                grow_reservation(reservation, path_state_bytes(&state))?;
                queue.push_back(state);
            }
            Ok::<(), GfError>(())
        })??;
        reservation.shrink(path_state_bytes(&p));
    }
    if cfg.capture_epoch != 0 {
        demand::record_candidates(cfg.capture_epoch, cfg.edge_var, examined);
    }
    Ok(emissions)
}

/// The public identity of one edge, for the edge-list column (#709). UUIDs +
/// relation type only — never the surrogate `*_id` columns (UUID-only contract).
pub(super) struct EdgeRecord {
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
        let edge_id_index = batch
            .schema()
            .index_of("edge_id")
            .map_err(|e| exec_err(e.to_string()))?;
        let edge_ids = u64_column(batch, edge_id_index)?;
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
pub(super) fn build_edge_list_column(
    cfg: &ExpandConfig,
    emissions: &[(usize, u64, Vec<u64>)],
    edge_records: &std::collections::HashMap<u64, EdgeRecord>,
    edge_props: &EdgePropSource,
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
    children.extend(edge_prop_children(edge_props, &prop_fields, &edge_uuids)?);

    let struct_arr =
        StructArray::try_new(struct_fields, children, None).map_err(|e| exec_err(e.to_string()))?;

    let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
    let list = ListArray::try_new(item.clone(), offsets, Arc::new(struct_arr), None)
        .map_err(|e| exec_err(e.to_string()))?;
    Ok(Arc::new(list))
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

pub(super) struct V4OrdinalIdentityPin {
    pub(super) session: Option<Arc<V4OrdinalIdentitySession>>,
    pub(super) required: bool,
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

    /// The exact generation this resolver serves, revalidated (by stat, no
    /// artifact bytes) for one caller outside query execution, such as search
    /// membership projection. `None` when the facade has no ordinal authority.
    ///
    /// # Errors
    /// Returns the refusal when the retained artifacts no longer match the
    /// generation the handle authenticated.
    pub fn revalidated_handle(
        &self,
    ) -> Result<
        Option<Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>>,
        GfError,
    > {
        let handle = self
            .handle
            .read()
            .expect("ordinal identity lock poisoned")
            .clone();
        if let Some(handle) = &handle {
            handle
                .lock()
                .expect("ordinal identity handle poisoned")
                .revalidate_for_session()
                .map_err(GfError::from)?;
        }
        Ok(handle)
    }

    pub(super) fn pin(&self) -> Result<V4OrdinalIdentityPin, GfError> {
        let handle = self
            .handle
            .read()
            .expect("ordinal identity lock poisoned")
            .clone();
        let Some(handle) = handle else {
            return Ok(V4OrdinalIdentityPin {
                session: None,
                required: false,
            });
        };
        let revalidation = handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .revalidate_for_session()
            .map_err(GfError::from)?;
        Ok(V4OrdinalIdentityPin {
            session: Some(Arc::new(V4OrdinalIdentitySession {
                handle,
                revalidation,
                attribution_available: AtomicBool::new(true),
            })),
            required: true,
        })
    }
}

/// One exact, already-authenticated ordinal authority pinned for the lifetime
/// of an execution session.
#[derive(Debug)]
pub(crate) struct V4OrdinalIdentitySession {
    handle: Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>,
    revalidation: graphforge_storage::V4OrdinalRevalidationMetrics,
    attribution_available: AtomicBool,
}

impl V4OrdinalIdentitySession {
    pub(crate) fn max_requested_ids(&self) -> usize {
        self.handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .max_requested_ids()
    }

    /// Whether node-ordinal order is UUID order. The first call reads and
    /// authenticates every ordinal block once; a corrupted block is an error,
    /// never an "unordered" answer.
    pub(crate) fn uuid_order_matches_ordinals(&self) -> Result<bool, GfError> {
        self.handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .uuid_order_matches_ordinals()
            .map_err(GfError::from)
    }

    pub(crate) fn lookup_node_uuids(
        &self,
        requested: &[u64],
    ) -> Result<graphforge_storage::V4OrdinalLookup, GfError> {
        let mut lookup = self
            .handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .lookup_node_uuids_pinned(requested)
            .map_err(GfError::from)?;
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
/// layout) the join chain would have produced. For `Undirected` this node
/// reads the provider's merged view and skips an edge already emitted for the
/// same source row, so a self-loop's two entries yield one row.
pub struct ExpandExec {
    pub(super) input: Arc<dyn ExecutionPlan>,
    rel_type_name: String,
    direction: Direction,
    dir: PathBuf,
    mode: OntologyMode,
    /// Column index of the source variable's `node_id` in the input
    /// (qualifier-resolved at construction, like `VarLenExpandExec`).
    pub(super) src_col_idx: usize,
    /// How many trailing `var_<edge>` schema fields are edge-property columns.
    edge_prop_count: usize,
    /// Number of columns contributed by the input (schema prefix length).
    pub(super) input_width: usize,
    /// Exact qualified destination UUID output slot from the logical expand.
    selected_destination_uuid_index: usize,
    pub(super) schema: SchemaRef,
    props: Arc<PlanProperties>,
    provider: Arc<dyn AdjacencyProvider>,
    /// Maximum rows this operator should emit after physical limit pushdown.
    pub(super) fetch: Option<usize>,
    /// Edge binding id used as a stable diagnostic hop key.
    edge_var: u32,
    /// Initial resumable output-batch goal propagated through selective filters.
    demand_batch: Option<usize>,
    /// Query-scoped terminal cancellation shared by the bounded hop chain.
    demand: Option<Arc<demand::QueryDemand>>,
    /// Capture session stamped when demand instrumentation attached this node.
    /// Deferred partition execution must not re-read the global epoch.
    capture_epoch: u64,
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
        resource: &read_resource::GraphReadContext,
    ) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = partitioned_properties(&schema, &input);
        let src_qual = datafusion::common::TableReference::bare(format!("var_{}", node.src_var));
        let src_col_idx = node
            .input
            .schema()
            .index_of_column_by_name(Some(&src_qual), "node_id")
            .unwrap_or(0);
        let dst_qual = datafusion::common::TableReference::bare(format!("var_{}", node.dst_var));
        let selected_destination_uuid_index = node
            .schema()
            .index_of_column_by_name(Some(&dst_qual), "node_uuid")
            .unwrap_or(usize::MAX);
        Self {
            input,
            rel_type_name: node.rel_type_name.clone(),
            direction: node.direction,
            dir: resource.dir.clone(),
            mode: resource.mode,
            src_col_idx,
            edge_prop_count: node.edge_prop_count,
            input_width: node.input.schema().fields().len(),
            selected_destination_uuid_index,
            schema,
            props,
            provider,
            fetch: None,
            edge_var: node.edge_var,
            demand_batch: None,
            demand: None,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
            required_output: None,
            ordinal_identities,
            ordinal_identity_required,
        }
    }

    pub(super) fn with_demand(
        &self,
        batch_goal: usize,
        demand: Arc<demand::QueryDemand>,
    ) -> Arc<dyn ExecutionPlan> {
        let capture_epoch = demand.capture_epoch();
        Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            selected_destination_uuid_index: self.selected_destination_uuid_index,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            fetch: self.fetch,
            edge_var: self.edge_var,
            demand_batch: Some(batch_goal),
            demand: Some(demand),
            capture_epoch,
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        })
    }

    pub(super) fn with_required_output(&self, required: Vec<bool>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            selected_destination_uuid_index: self.selected_destination_uuid_index,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            fetch: self.fetch,
            edge_var: self.edge_var,
            demand_batch: self.demand_batch,
            demand: self.demand.clone(),
            capture_epoch: self.capture_epoch,
            required_output: Some(required.into()),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        })
    }

    /// The owned per-stream configuration of one `execute` call.
    fn single_hop_config(&self, context: &TaskContext) -> SingleHopConfig {
        SingleHopConfig {
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
            capture_epoch: self.capture_epoch,
            demand: self.demand.clone(),
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
            reservation: std::sync::Mutex::new(crate::fast_path::expand_reservation(context)),
        }
    }
}

/// Whether `column_index` is the exact UUID emitted for the selected
/// destination by an expansion, after following only direct-column
/// projections and schema-preserving unary operators.
pub(crate) fn is_selected_endpoint_uuid(plan: &dyn ExecutionPlan, column_index: usize) -> bool {
    if let Some(expand) = plan.downcast_ref::<ExpandExec>() {
        let seed_proof = expand.children().first().is_some_and(|seed| {
            graphforge_storage::is_filtered_uuid_seed(seed.as_ref(), expand.src_col_idx)
        });
        return expand.selected_destination_uuid_index == column_index && seed_proof;
    }

    if let Some(projection) =
        plan.downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>()
    {
        let Some(expression) = projection.expr().get(column_index) else {
            return false;
        };
        let Some(column) = expression
            .expr
            .downcast_ref::<datafusion::physical_expr::expressions::Column>()
        else {
            return false;
        };
        return projection
            .input()
            .schema()
            .fields()
            .get(column.index())
            .is_some_and(|field| {
                field.name() == column.name()
                    && is_selected_endpoint_uuid(projection.input().as_ref(), column.index())
            });
    }

    if let Some(filter) = plan.downcast_ref::<datafusion::physical_plan::filter::FilterExec>() {
        // A filter only drops rows. When it also narrows its columns, follow
        // the requested column to the input column it passes through.
        let input_index = match filter.projection() {
            None => {
                if filter.schema().as_ref() != filter.input().schema().as_ref() {
                    return false;
                }
                column_index
            }
            Some(projection) => {
                let Some(&input_index) = projection.get(column_index) else {
                    return false;
                };
                let output = filter.schema();
                let input = filter.input().schema();
                let (Some(out_field), Some(in_field)) = (
                    output.fields().get(column_index),
                    input.fields().get(input_index),
                ) else {
                    return false;
                };
                if out_field.as_ref() != in_field.as_ref() {
                    return false;
                }
                input_index
            }
        };
        return is_selected_endpoint_uuid(filter.input().as_ref(), input_index);
    }
    if let Some(var_len) = plan.downcast_ref::<VarLenExpandExec>() {
        // The destination identity of a variable-length expansion seeded by an
        // exact UUID equality: every emitted destination is reached from that
        // seed, so the nominated frontier is exactly the reached set.
        let destination_uuid = var_len.input.schema().fields().len()
            + graphforge_storage::TOPOLOGY_NODES_SCHEMA
                .index_of("node_uuid")
                .unwrap_or(usize::MAX / 2);
        // Exchanges only move rows between partitions.
        let mut seed: &dyn ExecutionPlan = var_len.input.as_ref();
        loop {
            if let Some(exchange) =
                seed.downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>()
            {
                seed = exchange.input().as_ref();
            } else if let Some(coalesce) = seed.downcast_ref::<
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec,
            >() {
                seed = coalesce.input().as_ref();
            } else {
                break;
            }
        }
        let seed_proof = graphforge_storage::is_filtered_uuid_seed(seed, var_len.src_col_idx);
        return destination_uuid == column_index && seed_proof;
    }
    if let Some(exchange) =
        plan.downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>()
    {
        return exchange.schema().as_ref() == exchange.input().schema().as_ref()
            && is_selected_endpoint_uuid(exchange.input().as_ref(), column_index);
    }
    if let Some(coalesce) = plan
        .downcast_ref::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>(
    ) {
        return coalesce.schema().as_ref() == coalesce.input().schema().as_ref()
            && is_selected_endpoint_uuid(coalesce.input().as_ref(), column_index);
    }
    false
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
            "ExpandExec: rel={}, dir={arrow}, adjacency={}, identity={}, fetch={}, demand_batch={}, projection={}, cancel={}",
            self.rel_type_name,
            self.provider
                .explain_status(&self.rel_type_name, self.direction),
            if self.ordinal_identities.is_some() {
                "v4"
            } else if self.ordinal_identity_required {
                "required-missing"
            } else {
                "legacy"
            },
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
        let props = partitioned_properties(&self.schema, &input);
        if self.fetch.is_some() && props.output_partitioning().partition_count() > 1 {
            return Err(DataFusionError::Internal(
                "ExpandExec fetch bounds one output stream and cannot span several partitions"
                    .into(),
            ));
        }
        Ok(Arc::new(Self {
            input,
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            selected_destination_uuid_index: self.selected_destination_uuid_index,
            schema: self.schema.clone(),
            props,
            provider: self.provider.clone(),
            fetch: self.fetch,
            edge_var: self.edge_var,
            demand_batch: self.demand_batch,
            demand: self.demand.clone(),
            capture_epoch: self.capture_epoch,
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        }))
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        // `fetch` bounds the rows of one output stream. Across several
        // partitions each would emit up to `fetch` rows, so the limit stays in
        // the operator above, which sees every partition.
        if self.props.output_partitioning().partition_count() > 1 {
            return None;
        }
        Some(Arc::new(Self {
            input: Arc::clone(&self.input),
            rel_type_name: self.rel_type_name.clone(),
            direction: self.direction,
            dir: self.dir.clone(),
            mode: self.mode,
            src_col_idx: self.src_col_idx,
            edge_prop_count: self.edge_prop_count,
            input_width: self.input_width,
            selected_destination_uuid_index: self.selected_destination_uuid_index,
            schema: Arc::clone(&self.schema),
            props: Arc::clone(&self.props),
            provider: Arc::clone(&self.provider),
            fetch,
            edge_var: self.edge_var,
            demand_batch: self.demand_batch,
            demand: self.demand.clone(),
            capture_epoch: self.capture_epoch,
            required_output: self.required_output.clone(),
            ordinal_identities: self.ordinal_identities.clone(),
            ordinal_identity_required: self.ordinal_identity_required,
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // The hop runs on the partitions its input already has. A planner
        // exchange is inserted only where another rule asks for one; a
        // single-partition input keeps its plan shape, which the fixed-hop fast
        // paths rely on.
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        check_partition("ExpandExec", &self.input, partition)?;
        let input = self.input.clone();
        let cfg = self.single_hop_config(&context);
        let schema = self.schema.clone();
        let batch_size = context.session_config().batch_size();
        let input_stream = input.execute(partition, context)?;
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
                        cfg.account(pending.as_ref().map(|(batch, _)| batch), &output)?;
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
                    demand::record_input(cfg.capture_epoch, cfg.edge_var, input_batch.num_rows());
                    cfg.account(
                        Some(&input_batch),
                        &RecordBatch::new_empty(cfg.out_schema.clone()),
                    )?;
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
    capture_epoch: u64,
    demand: Option<Arc<demand::QueryDemand>>,
    required_output: Option<Arc<[bool]>>,
    ordinal_identities: Option<Arc<V4OrdinalIdentitySession>>,
    ordinal_identity_required: bool,
    /// #1688 candidate C: the memory-pool reservation for held batches.
    /// Memory-pool reservation for the batches this hop holds (ADR 0050).
    reservation: std::sync::Mutex<datafusion::execution::memory_pool::MemoryReservation>,
}

impl SingleHopConfig {
    /// Charge the held input batch and the output batch to the session memory pool.
    fn account(
        &self,
        held: Option<&RecordBatch>,
        output: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        let bytes =
            held.map_or(0, RecordBatch::get_array_memory_size) + output.get_array_memory_size();
        self.reservation
            .lock()
            .map_err(|_| DataFusionError::Internal("ExpandExec reservation poisoned".into()))?
            .try_resize(bytes)
    }
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
            .map_err(GfError::from_execution_error)?,
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

fn require_admitted_ordinal_identity(required: bool, admitted: bool) -> Result<(), GfError> {
    if required && !admitted {
        return Err(GfError::Execution(
            "destination UUID projection requires admitted v4 ordinal identity".into(),
        ));
    }
    Ok(())
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
    let mut adjacency = adjacency::AdjacencyReader::for_capture(
        cfg.provider.as_ref(),
        &cfg.rel_type_name,
        cfg.direction,
        cfg.capture_epoch,
    )?;

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
        adjacency.with_neighbors(src, |neighbors| {
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
        })?;
    }
    if triples.is_empty() {
        return Ok(RecordBatch::new_empty(cfg.out_schema.clone()));
    }
    demand::record_candidates(cfg.capture_epoch, cfg.edge_var, triples.len());

    let dst_width = graphforge_storage::TOPOLOGY_NODES_SCHEMA.fields().len();
    let edge_end = cfg.out_schema.fields().len().saturating_sub(dst_width);
    let required = cfg.required_output.as_deref();
    let edge_topology_width = edge_end
        .saturating_sub(cfg.input_width)
        .saturating_sub(cfg.edge_prop_count);
    let edge_topology_end = cfg.input_width + edge_topology_width;
    // The edge file is needed only for what the CSR does not carry: the edge's
    // UUID and endpoints, and its properties. The CSR entry already holds the
    // edge's ordinal `edge_id`, so a hop whose edge variable is demanded for
    // nothing beyond that ordinal reads no edge file (#1984).
    let ordinal_topology_only = required.is_some_and(|mask| {
        mask.get(cfg.input_width..edge_topology_end)
            .is_some_and(|fields| {
                fields.iter().enumerate().all(|(offset, needed)| {
                    !needed || cfg.out_schema.field(cfg.input_width + offset).name() == "edge_id"
                })
            })
    });
    let relationship_properties_required = required.is_none_or(|mask| {
        mask[edge_topology_end..edge_end]
            .iter()
            .any(|needed| *needed)
    });
    let edge_materialization_unused = ordinal_topology_only && !relationship_properties_required;
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
    if edge_materialization_unused && destination_identity_only && uuid_required {
        require_admitted_ordinal_identity(
            cfg.ordinal_identity_required,
            cfg.ordinal_identities.is_some(),
        )?;
    }
    if edge_materialization_unused
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
        for (offset, field) in cfg
            .out_schema
            .fields()
            .iter()
            .skip(cfg.input_width)
            .take(edge_end.saturating_sub(cfg.input_width))
            .enumerate()
        {
            let index = cfg.input_width + offset;
            if required.is_some_and(|mask| mask[index]) && field.name() == "edge_id" {
                columns.push(Arc::new(UInt64Array::from(
                    triples
                        .iter()
                        .map(|(_, edge_id, _)| *edge_id)
                        .collect::<Vec<_>>(),
                )));
            } else {
                columns.push(unused_expand_column(field, triples.len())?);
            }
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
            cfg.capture_epoch,
            cfg.edge_var,
            output.num_rows(),
            projected_columns,
            &identity_metrics,
        );
        demand::record_emitted(cfg.capture_epoch, cfg.edge_var, output.num_rows());
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
    let edge_observer = demand::session_active_for(cfg.capture_epoch).then(|| {
        Arc::new(demand::HopReadObserver::with_epoch(
            cfg.edge_var,
            cfg.capture_epoch,
        )) as Arc<dyn graphforge_storage::io_stats::FilteredReadObserver>
    });
    let prop_fields: Vec<arrow::datatypes::FieldRef> = cfg
        .out_schema
        .fields()
        .iter()
        .skip(edge_topology_end)
        .take(cfg.edge_prop_count)
        .cloned()
        .collect();
    let demanded_property_fields = prop_fields
        .iter()
        .enumerate()
        .filter(|(offset, _)| required.is_none_or(|mask| mask[edge_topology_end + offset]))
        .map(|(_, field)| Arc::clone(field))
        .collect::<Vec<_>>();
    // A hop that reads the edge only for its ordinal and its properties finds
    // the properties by the `edge_id` the CSR already holds, in a property index
    // addressed by edge id: the edge file, and its 16-byte `edge_uuid` column,
    // are not read at all (#1984). A route whose index cannot answer by id
    // (older, stale, or not joined to its topology) keeps the UUID-keyed read.
    let id_keyed_properties = if relationship_properties_required && ordinal_topology_only {
        let targets: std::collections::BTreeSet<u64> = traversed.iter().copied().collect();
        edge_properties::load_edge_prop_source_by_id(
            cfg.provider.admitted_inventory().as_deref(),
            &cfg.rel_type_name,
            &cfg.dir,
            &demanded_property_fields,
            &targets,
        )?
    } else {
        None
    };
    let mut edge_projection = (0..edge_topology_width)
        .filter(|offset| required.is_none_or(|mask| mask[cfg.input_width + offset]))
        .collect::<Vec<_>>();
    // edge_id keys adjacency entries; edge_uuid keys demanded relationship
    // properties. Storage adds edge_id automatically.
    if relationship_properties_required {
        edge_projection.push(0);
    }
    // A hop whose edge variable is demanded for nothing beyond its ordinal (and
    // properties found by that ordinal) reads no edge file: the CSR entry
    // already supplies `edge_id` (#1984).
    let edge_batch = if edge_materialization_unused || id_keyed_properties.is_some() {
        drop(edge_permit);
        None
    } else {
        let admitted_inventory = cfg.provider.admitted_inventory();
        let edge_batches =
            if let Some(inventory) = admitted_inventory.as_deref().filter(|_| required.is_some()) {
                graphforge_storage::read_edges_filtered_projected_from_inventory(
                    inventory,
                    &cfg.rel_type_name,
                    cfg.mode,
                    &traversed,
                    &edge_projection,
                    edge_observer.as_ref(),
                )
            } else if required.is_some() {
                graphforge_storage::read_edges_filtered_projected_observed(
                    &cfg.dir,
                    &cfg.rel_type_name,
                    cfg.mode,
                    &traversed,
                    &edge_projection,
                    edge_observer.as_ref(),
                )
            } else if let Some(inventory) = admitted_inventory.as_deref() {
                graphforge_storage::read_edges_filtered_observed_from_inventory(
                    inventory,
                    &cfg.rel_type_name,
                    cfg.mode,
                    &traversed,
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
        Some(concat_batches(&edge_schema, &edge_batches).map_err(|e| exec_err(e.to_string()))?)
    };
    // Position of each traversed edge within the edge batch, by `edge_id`.
    let edge_row: HashMap<u64, usize> = match edge_batch.as_ref() {
        Some(edge_batch) => {
            let edge_id_index = edge_batch
                .schema()
                .index_of("edge_id")
                .map_err(|error| exec_err(error.to_string()))?;
            let edge_ids_col = u64_column(edge_batch, edge_id_index)?;
            (0..edge_batch.num_rows())
                .filter_map(|i| edge_ids_col.value_at(i).map(|id| (id, i)))
                .collect()
        }
        None => HashMap::new(),
    };
    let edge_uuids = edge_batch
        .as_ref()
        .filter(|_| relationship_properties_required)
        .map(|edge_batch| {
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
    let node_observer = demand::session_active_for(cfg.capture_epoch).then(|| {
        Arc::new(demand::HopReadObserver::with_epoch(
            cfg.edge_var,
            cfg.capture_epoch,
        )) as Arc<dyn graphforge_storage::io_stats::FilteredReadObserver>
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
    let edge_projected_columns = if edge_batch.is_some() {
        edge_projection
            .len()
            .saturating_add(1_usize.saturating_sub(edge_key_already_demanded))
            .saturating_add(required.map_or(cfg.edge_prop_count, |mask| {
                mask[cfg.input_width + edge_topology_width..edge_end]
                    .iter()
                    .filter(|needed| **needed)
                    .count()
            }))
    } else {
        0
    };
    demand::record_materialization_projection(
        cfg.capture_epoch,
        cfg.edge_var,
        edge_projected_columns,
        node_projection
            .len()
            .saturating_add(1_usize.saturating_sub(node_key_already_demanded)),
    );
    let node_batches = if required.is_some() {
        graphforge_storage::read_nodes_filtered_projected_observed_from_files(
            &topology_for_provider(cfg.provider.as_ref(), &cfg.dir)?,
            &reached,
            &node_projection,
            node_observer.as_ref(),
        )
    } else {
        graphforge_storage::read_nodes_filtered_observed_from_files(
            &topology_for_provider(cfg.provider.as_ref(), &cfg.dir)?,
            &reached,
            node_observer.as_ref(),
        )
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
        let Some(&dst_idx) = node_row.get(&neighbor) else {
            return Err(exec_err(format!(
                "Expand reached unknown destination node_id {neighbor}"
            )));
        };
        src_take.push(to_u32(row)?);
        dst_take.push(to_u32(dst_idx)?);
        if edge_batch.is_none() {
            continue;
        }
        let Some(&edge_idx) = edge_row.get(&edge_id) else {
            return Err(exec_err(format!(
                "Expand: adjacency entry references unknown edge_id {edge_id}"
            )));
        };
        edge_take.push(to_u32(edge_idx)?);
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
        columns.push(if !required.is_none_or(|mask| mask[index]) {
            unused_expand_column(field, triples.len())?
        } else if let Some(edge_batch) = edge_batch.as_ref() {
            let column = edge_batch.column_by_name(field.name()).ok_or_else(|| {
                exec_err(format!(
                    "Expand: projected edge column {} is absent",
                    field.name()
                ))
            })?;
            take(column, &edge_take, None).map_err(|error| exec_err(error.to_string()))?
        } else {
            // No edge file was read: the only demanded edge column is the
            // ordinal the CSR carries.
            Arc::new(UInt64Array::from(
                triples
                    .iter()
                    .map(|(_, edge_id, _)| *edge_id)
                    .collect::<Vec<_>>(),
            ))
        });
    }
    let demanded_property_columns = if let Some(source) = id_keyed_properties.as_ref() {
        let hop_edge_ids = triples
            .iter()
            .map(|(_, edge_id, _)| *edge_id)
            .collect::<Vec<_>>();
        edge_properties::edge_prop_children_by_id(source, &demanded_property_fields, &hop_edge_ids)?
    } else {
        build_edge_prop_children(
            cfg.provider.admitted_inventory().as_deref(),
            &cfg.rel_type_name,
            &cfg.dir,
            &demanded_property_fields,
            &output_edge_uuids,
        )?
    };
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
    demand::record_emitted(cfg.capture_epoch, cfg.edge_var, output.num_rows());
    Ok(output)
}

mod edge_filter;
mod edge_properties;
mod reachability;
#[cfg(test)]
mod tests;

fn topology_for_provider(
    provider: &dyn AdjacencyProvider,
    legacy_root: &Path,
) -> Result<graphforge_storage::TopologyFiles, GfError> {
    match provider.admitted_inventory() {
        Some(inventory) => graphforge_storage::TopologyFiles::from_inventory(&inventory),
        None => graphforge_storage::TopologyFiles::discover_legacy(legacy_root),
    }
}
