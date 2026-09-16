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
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_ir::Direction;
use graphforge_plan::VarLenExpandNode;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

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
        dir: PathBuf,
        mode: OntologyMode,
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
            dir,
            mode,
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
    pub(super) out_schema: SchemaRef,
    /// Adjacency source (#762) — moved into the `'static` execute future.
    pub(super) provider: Arc<dyn AdjacencyProvider>,
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
    let mut adjacency =
        adjacency::AdjacencyReader::new(cfg.provider.as_ref(), &cfg.rel_type_name, cfg.direction)?;

    // --- BFS per source row, with per-path edge deduplication. ---
    // Run the traversal BEFORE any edge-file read: the BFS needs only the
    // adjacency view, and knowing the traversed edge ids lets the
    // relationship-list read below fetch exactly those rows (#830) instead of
    // scanning the whole file.
    let emissions = bfs_emit(cfg, &mut adjacency, src_ids)?;
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
    let edge_batches = match cfg.provider.admitted_inventory() {
        Some(inventory) => graphforge_storage::read_edges_filtered_from_inventory(
            &inventory,
            &cfg.rel_type_name,
            cfg.mode,
            &traversed,
        ),
        None => graphforge_storage::read_edges_filtered(
            &cfg.dir,
            &cfg.rel_type_name,
            cfg.mode,
            &traversed,
        ),
    }
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
    adjacency: &mut adjacency::AdjacencyReader<'_>,
    src_ids: &arrow::array::UInt64Array,
) -> Result<Vec<(usize, u64, Vec<u64>)>, GfError> {
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
        adjacency.with_neighbors(p.node, |neighbors| {
            for (edge_id, next) in neighbors.iter() {
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
        })?;
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
pub(super) fn build_edge_list_column(
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
        cfg.provider.admitted_inventory().as_deref(),
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

fn read_target_edge_properties(
    inventory: &graphforge_storage::AuthenticatedPropertyInventory,
    stem: &str,
    targets: &std::collections::BTreeSet<[u8; 16]>,
    property_names: &[String],
    owners: &mut std::collections::BTreeSet<[u8; 16]>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, GfError> {
    let selected = graphforge_storage::read_authenticated_property_targets_for_inventory(
        inventory,
        graphforge_storage::PropertyRouteKind::Edge,
        stem,
        targets,
    )?;
    if selected.present.iter().any(|uuid| !owners.insert(*uuid)) {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message: "edge properties have multiple authenticated owners".into(),
        });
    }
    let schema = inventory.route_schema(graphforge_storage::PropertyRouteKind::Edge, stem);
    match schema {
        Some(schema) => {
            let indices = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| {
                    field.name() == "edge_uuid" || property_names.contains(field.name())
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let projected = schema
                .project(&indices)
                .map_err(GfError::from_execution_error)?;
            Ok(vec![selected.edge_batch(&projected)?])
        }
        None => Ok(Vec::new()),
    }
}

fn edge_property_stems(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    dir: &Path,
    rel_type_name: &str,
) -> Vec<String> {
    if rel_type_name == "*" {
        match inventory {
            Some(inventory) => inventory
                .routes(graphforge_storage::PropertyRouteKind::Edge)
                .map(str::to_owned)
                .collect(),
            None => graphforge_storage::list_edge_property_stems(dir),
        }
    } else {
        let mut candidates = vec![rel_type_name.to_owned()];
        let has_shared = match inventory {
            Some(inventory) => inventory
                .route_schema(graphforge_storage::PropertyRouteKind::Edge, "_exploratory")
                .is_some(),
            None => graphforge_storage::list_edge_property_stems(dir)
                .iter()
                .any(|stem| stem == "_exploratory"),
        };
        if rel_type_name != "_exploratory" && has_shared {
            candidates.push("_exploratory".to_owned());
        }
        candidates.sort();
        candidates
    }
}

/// Build one child array per edge-property struct field (#755), in field order.
///
/// Authenticates candidate owners and retains values for flattened hop UUIDs.
/// Named traversals select the logical route and shared construction route;
/// wildcard traversals select every route. Tombstones participate in ownership
/// refusal but never contribute values. Missing properties become NULL under
/// the lowering's nullable union schema. Returns no children without properties.
/// `hop_edge_uuids` is in flattened hop order (matching the topology children).
fn build_edge_prop_children(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
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

    // Resolve selected rows once per candidate route, then assemble hop order.
    let stems = edge_property_stems(inventory, dir, rel_type_name);
    let targets = hop_edge_uuids.iter().copied().collect();
    let mut owners = std::collections::BTreeSet::new();
    let mut prop_batches_by_rel = Vec::with_capacity(stems.len());
    let property_names = prop_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    for stem in &stems {
        let batches = match inventory {
            Some(inventory) => {
                read_target_edge_properties(inventory, stem, &targets, &property_names, &mut owners)
            }
            None => graphforge_storage::read_edge_properties_projected(dir, stem, &property_names)
                .map_err(|e| exec_err(e.to_string())),
        }?;
        if let Some(first) = batches.first() {
            prop_batches_by_rel.push(
                concat_batches(&first.schema(), &batches).map_err(|e| exec_err(e.to_string()))?,
            );
        }
    }

    // edge_uuid -> (owning batch, row within it). An edge belongs to exactly
    // one physical owner; duplicate ownership is corruption, not precedence.
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
            let location = (
                bi,
                u32::try_from(r)
                    .map_err(|_| exec_err(format!("edge-property row {r} exceeds u32")))?,
            );
            if uuid_to_loc.insert(u, location).is_some() {
                return Err(exec_err(
                    "edge properties have multiple physical owners".into(),
                ));
            }
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
            // A route whose last value was removed advertises Null. Its
            // contribution is null in the concrete union type as well.
            if col.data_type() == &arrow::datatypes::DataType::Null {
                continue;
            }
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
            .map_err(GfError::from_execution_error)?;
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

    pub(crate) fn uuid_order_matches_ordinals(&self) -> bool {
        self.handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .uuid_order_matches_ordinals()
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
            .map_err(GfError::from_execution_error)?;
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
            dir: resource.dir.clone(),
            mode: resource.mode,
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

    pub(crate) fn rel_type_name(&self) -> &str {
        &self.rel_type_name
    }

    pub(crate) fn direction(&self) -> graphforge_ir::Direction {
        self.direction
    }

    pub(crate) fn provider(&self) -> &Arc<dyn AdjacencyProvider> {
        &self.provider
    }

    pub(crate) fn ordinal_identities(&self) -> Option<Arc<V4OrdinalIdentitySession>> {
        self.ordinal_identities.clone()
    }

    pub(crate) fn is_destination_identity_only(&self) -> bool {
        self.is_identity_projection_only(true)
    }

    pub(crate) fn is_intermediate_topology_only(&self) -> bool {
        self.is_identity_projection_only(false)
    }

    fn is_identity_projection_only(&self, require_destination_uuid: bool) -> bool {
        let Some(required) = self.required_output.as_deref() else {
            return false;
        };
        let dst_width = graphforge_storage::TOPOLOGY_NODES_SCHEMA.fields().len();
        let edge_end = self.schema.fields().len().saturating_sub(dst_width);
        let destination_uuid_index = edge_end;
        let destination_id_index = edge_end + 1;
        let edge_materialization_unused =
            required
                .get(self.input_width..edge_end)
                .is_some_and(|fields| {
                    fields.iter().enumerate().all(|(offset, needed)| {
                        !needed || self.schema.field(self.input_width + offset).name() == "edge_id"
                    })
                });
        let required_destination = if require_destination_uuid {
            destination_uuid_index
        } else {
            destination_id_index
        };
        edge_materialization_unused
            && required
                .iter()
                .enumerate()
                .skip(edge_end)
                .all(|(index, needed)| {
                    !needed || index == destination_uuid_index || index == destination_id_index
                })
            && required.get(required_destination).copied().unwrap_or(false)
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
            "ExpandExec: rel={}, dir={arrow}, adjacency={}, identity={}, fetch={}, demand_batch={}, projection={}, cancel={}",
            self.rel_type_name,
            self.provider
                .status(&self.rel_type_name, self.direction)
                .as_str(),
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
            capture_epoch: self.capture_epoch,
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
            capture_epoch: self.capture_epoch,
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
                    demand::record_input(cfg.capture_epoch, cfg.edge_var, input_batch.num_rows());
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
    let mut adjacency =
        adjacency::AdjacencyReader::new(cfg.provider.as_ref(), &cfg.rel_type_name, cfg.direction)?;

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
    let edge_materialization_unused = required.is_some_and(|mask| {
        mask.get(cfg.input_width..edge_end).is_some_and(|fields| {
            fields.iter().enumerate().all(|(offset, needed)| {
                !needed || cfg.out_schema.field(cfg.input_width + offset).name() == "edge_id"
            })
        })
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
    demand::record_materialization_projection(
        cfg.capture_epoch,
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
        cfg.provider.admitted_inventory().as_deref(),
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
    demand::record_emitted(cfg.capture_epoch, cfg.edge_var, output.num_rows());
    Ok(output)
}

#[cfg(test)]
mod tests;
