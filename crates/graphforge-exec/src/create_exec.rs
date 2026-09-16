//! CREATE physical execution and shared statement creation.

use crate::ValueAt;
use crate::mutation;
use crate::to_df_err;
use crate::u64_column;
use crate::write_driver;
use crate::write_resource;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::RecordBatch;
use arrow::array::StructArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::common::DFSchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_plan::GraphCreateNode;
use graphforge_plan::ResolvedEdgeSpec;
use graphforge_plan::ResolvedNodeSpec;
use graphforge_rel::scalar_to_ir_literal;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

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
    mutation_health: mutation::MutationHealth,
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
pub(super) struct RefNodeCols {
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

    pub(super) fn resolve_with_alias(schema: &DFSchema, var: u32, alias: &str) -> Option<Self> {
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
        field
            .name()
            .starts_with(graphforge_value::heterogeneous::DYNAMIC_PREFIX)
            && matches!(field.data_type(), DataType::Struct(value_fields)
                if value_fields.iter().any(|value_field| value_field.name() == "node_uuid"))
    })
}

impl GraphCreateExec {
    /// Build a physical CREATE node from its logical counterpart and planned
    /// input.
    ///
    /// # Errors
    /// Rejects a write resource incompatible with the logical binding contract.
    pub fn new(
        node: &GraphCreateNode,
        input: Arc<dyn ExecutionPlan>,
        resource: &write_resource::BoundWriteResource,
    ) -> Result<Self, DataFusionError> {
        resource.validate(node.write_contract.as_ref())?;
        resource.validate_composition(node.semantic_composition_fingerprint.as_deref())?;
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
        Ok(Self {
            input,
            nodes: node.nodes.clone(),
            edges: node.edges.clone(),
            ref_cols,
            in_df_schema: in_schema.clone(),
            dir: resource.dir.clone(),
            mutation_health: resource.health.clone(),
            mode: resource.mode,
            semantic_composition_fingerprint: node.semantic_composition_fingerprint.clone(),
            schema,
            emit_rows,
            effects: Arc::new(std::sync::Mutex::new(CreateTally::default())),
            props,
        })
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

    /// Owned config for [`write_batch_creates`] so the writes can run in a `'static`
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
            mutation_health: self.mutation_health.clone(),
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

        self.mutation_health
            .check()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

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
        Ok(self
            .mutation_health
            .guard_stream(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::once(fut),
            ))))
    }
}

/// Owned configuration for [`write_batch_creates`].
pub(crate) struct CreateConfig {
    pub(crate) nodes: Vec<ResolvedNodeSpec>,
    pub(crate) edges: Vec<ResolvedEdgeSpec>,
    pub(super) ref_cols: Vec<RefNodeCols>,
    /// Input logical schema, for building physical exprs from the specs'
    /// row-dependent computed property values (#814).
    pub(crate) in_df_schema: DFSchemaRef,
    pub(super) dir: PathBuf,
    pub(super) mode: OntologyMode,
    pub(super) semantic_composition_fingerprint: Option<String>,
    pub(super) out_schema: SchemaRef,
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
pub(super) fn validate_edge_specs(cfg: &CreateConfig) -> Result<(), GfError> {
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
pub(super) fn build_ref_by_var(cfg: &CreateConfig) -> std::collections::HashMap<u32, &RefNodeCols> {
    cfg.ref_cols.iter().map(|r| (r.var, r)).collect()
}

pub(super) fn persisted_node_ids(
    dir: &Path,
) -> Result<std::collections::HashMap<[u8; 16], u64>, GfError> {
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
        let graphforge_value::heterogeneous::Decoded::Payload(payload) =
            graphforge_value::heterogeneous::decode_row(tagged, row)
                .map_err(|error| GfError::Execution(error.to_string()))?
        else {
            return Err(GfError::Execution(
                "CREATE node reference is null or not a node".into(),
            ));
        };
        let variant = payload
            .as_any()
            .downcast_ref::<StructArray>()
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
    .map_err(GfError::from_execution_error)
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
        .collect::<HashSet<graphforge_value::EntityTypeId>>()
        .len() as u64
}

/// Driver-supplied extras for the create phase (#792): reject references to
/// entities deleted earlier in the statement, and record minted identities so
/// the driver can extend its frontier. The `Default` is the single-clause
/// CREATE shape (nothing deleted, no recording).
#[derive(Default)]
pub(super) struct CreateExtras<'a> {
    pub(super) deleted: Option<&'a HashSet<[u8; 16]>>,
    pub(super) recorder: Option<&'a mut write_driver::CreateRecorder>,
    /// Per-var row-dependent property columns (evaluated by
    /// [`eval_create_computed`]); merged into each minted entity's props at its
    /// row index (#814).
    pub(super) computed: Option<&'a CreateComputed>,
    pub(super) persisted_ids: Option<&'a HashMap<[u8; 16], u64>>,
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
        let scalar =
            ScalarValue::try_from_array(array, row).map_err(GfError::from_execution_error)?;
        if scalar.is_null() {
            continue;
        }
        let lit = scalar_to_ir_literal(&scalar).map_err(GfError::from_execution_error)?;
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
pub(super) fn write_batch_creates(
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
                    rec.record_node(spec.var, to_bytes(&uuid), node_id, type_id);
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
) -> (
    Vec<graphforge_value::EntityTypeId>,
    graphforge_value::PrimaryEntityTypeId,
) {
    let labels = spec.label_ids.clone();
    let primary = labels.first().copied().map_or_else(
        graphforge_value::PrimaryEntityTypeId::absent,
        graphforge_value::PrimaryEntityTypeId::known,
    );
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
    RecordBatch::try_new(cfg.out_schema.clone(), out_cols).map_err(GfError::from_execution_error)
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
    let empty_type_ids: &[graphforge_value::PrimaryEntityTypeId] = &[];
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
            .map_err(GfError::from_execution_error)?;
    }
    out_cols.push(Arc::new(uuid_b.finish()));
    out_cols.push(Arc::new(UInt64Array::from(node_ids.to_vec())));
    out_cols.push(Arc::new(UInt32Array::from(
        type_ids.iter().map(|id| id.encode()).collect::<Vec<_>>(),
    )));
    out_cols.push(write_driver::repeated_label_sets(&spec.label_ids, rows));

    for (_, lit) in &spec.properties {
        let scalar = graphforge_rel::expr::ir_literal_to_scalar(lit);
        out_cols.push(
            scalar
                .to_array_of_size(rows)
                .map_err(GfError::from_execution_error)?,
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
pub(super) fn create_tally_in_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<CreateTally> {
    if let Some(c) = plan.downcast_ref::<GraphCreateExec>()
        && c.emits_rows()
    {
        return Some(c.effects());
    }
    plan.children().into_iter().find_map(create_tally_in_plan)
}

#[cfg(test)]
mod tests;
