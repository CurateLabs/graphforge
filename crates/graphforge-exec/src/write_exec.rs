//! DELETE, SET, and REMOVE physical execution.

use crate::UNTYPED_STEM;
use crate::fixed_binary_uuid;
use crate::mutation;
use crate::to_df_err;
use crate::write_resource;
use arrow::array::Array;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::common::DFSchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr as DfExpr;
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
use graphforge_ir::IrLiteral;
use graphforge_plan::DeleteTarget;
use graphforge_plan::GraphDeleteNode;
use graphforge_plan::GraphRemoveNode;
use graphforge_plan::GraphSetNode;
use graphforge_plan::RemoveTarget;
use graphforge_plan::SetTarget;
use graphforge_rel::scalar_to_ir_literal;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// GraphDeleteExec — physical node for DELETE / DETACH DELETE
// ---------------------------------------------------------------------------

/// Resolved input-column location of one delete target's identity column.
#[derive(Clone)]
pub(super) struct DeleteCol {
    /// Column index of `var_<n>.node_uuid` (node target) or `var_<n>.edge_uuid`
    /// (edge target) within the input schema.
    pub(super) uuid_idx: usize,
    pub(super) is_edge: bool,
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
    mutation_health: mutation::MutationHealth,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl GraphDeleteExec {
    /// Build the physical DELETE node from its logical counterpart and input.
    ///
    /// # Errors
    /// Rejects a write resource incompatible with the logical binding contract.
    pub fn new(
        node: &GraphDeleteNode,
        input: Arc<dyn ExecutionPlan>,
        resource: &write_resource::BoundWriteResource,
    ) -> Result<Self, DataFusionError> {
        resource.validate(node.write_contract.as_ref())?;
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
        Ok(Self {
            input,
            cols,
            detach: node.detach,
            dir: resource.dir.clone(),
            mutation_health: resource.health.clone(),
            schema,
            props,
        })
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
            mutation_health: self.mutation_health.clone(),
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

        self.mutation_health
            .check()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

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
        Ok(self
            .mutation_health
            .guard_stream(Box::pin(RecordBatchStreamAdapter::new(
                stream_schema,
                futures::stream::once(fut),
            ))))
    }
}

/// Collect one input batch's delete-target uuids into the node/edge sets —
/// the per-batch DELETE collection phase, shared by [`GraphDeleteExec`] and
/// the mixed-write statement driver (#792).
///
/// openCypher: DELETE of a NULL is a no-op. An unmatched OPTIONAL MATCH row
/// has a null identity column — skip it rather than letting
/// `fixed_binary_uuid` error.
pub(super) fn collect_delete_targets(
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
    .map_err(GfError::from_execution_error)
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
    .map_err(GfError::from_execution_error)
}

/// Resolve a write target's input-column locations: the identity UUID column and
/// (for a node) the `type_id` column, or (for an edge) the `rel_type_name`
/// column used to route the property file per row.
#[derive(Clone)]
pub(super) struct WriteCol {
    /// Property name being written / removed.
    prop_name: String,
    /// Column index of `var_<n>.node_uuid` / `var_<n>.edge_uuid`.
    pub(super) uuid_idx: usize,
    pub(super) is_edge: bool,
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
    pub(super) fn resolve(
        in_schema: &DFSchema,
        var: u32,
        is_edge: bool,
        prop_name: &str,
    ) -> Option<Self> {
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
    pub(super) fn stem_for_row(
        &self,
        batch: &RecordBatch,
        row: usize,
        mode: OntologyMode,
        type_id_to_entity_name: &HashMap<graphforge_value::EntityTypeId, String>,
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
        if arr.is_null(row) {
            return Err(GfError::Execution("node primary identity is null".into()));
        }
        let primary = graphforge_value::PrimaryEntityTypeId::decode(arr.value(row))
            .map_err(|error| GfError::Execution(error.to_string()))?;
        Ok(primary
            .label()
            .and_then(|id| type_id_to_entity_name.get(&id))
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
    pub(super) nodes: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>>,
    /// stem → uuid → { prop → value } for edge targets.
    pub(super) edges: HashMap<String, HashMap<[u8; 16], HashMap<String, IrLiteral>>>,
}

impl SetAccumulator {
    pub(super) fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }

    pub(super) fn record(
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

    pub(super) fn forget(&mut self, is_edge: bool, stem: &str, uuid: &[u8; 16], prop: &str) {
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
    pub(super) fn stage_into(
        &self,
        staged: &mut graphforge_storage::RewriteBatch,
        dir: &Path,
        inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    ) -> Result<u64, GfError> {
        if self.is_empty() {
            return Ok(0);
        }
        let captured;
        let inventory = if let Some(inventory) = inventory {
            inventory
        } else {
            captured = graphforge_storage::AuthenticatedPropertyInventory::capture(dir)?;
            &captured
        };
        let mut total = 0u64;
        for (stem, updates) in &self.nodes {
            total += graphforge_storage::stage_set_node_properties_authenticated(
                staged, dir, inventory, stem, updates,
            )?;
        }
        for (stem, updates) in &self.edges {
            total += graphforge_storage::stage_set_edge_properties_authenticated(
                staged, dir, inventory, stem, updates,
            )?;
        }
        Ok(total)
    }

    /// Apply all accumulated node + edge sets through the storage primitives
    /// as one staged batch (#790 — a failure leaves every stem untouched),
    /// returning the total number of distinct entities written.
    fn apply(&self, dir: &Path) -> Result<u64, GfError> {
        let mut staged = graphforge_storage::RewriteBatch::new();
        let total = self.stage_into(&mut staged, dir, None)?;
        staged.commit_at(dir)?;
        Ok(total)
    }

    /// Drop every accumulated write targeting a uuid in `deleted` (#792): the
    /// entity is gone by statement end, so its property writes are
    /// unobservable and must not resurrect file rows.
    pub(super) fn scrub(&mut self, deleted: &HashSet<[u8; 16]>) {
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
    pub(super) nodes: HashMap<String, HashMap<[u8; 16], HashSet<String>>>,
    pub(super) edges: HashMap<String, HashMap<[u8; 16], HashSet<String>>>,
}

impl RemoveAccumulator {
    pub(super) fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }

    pub(super) fn record(&mut self, is_edge: bool, stem: String, uuid: [u8; 16], prop: String) {
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

    pub(super) fn forget(&mut self, is_edge: bool, stem: &str, uuid: &[u8; 16], prop: &str) {
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
    pub(super) fn stage_into(
        &self,
        staged: &mut graphforge_storage::RewriteBatch,
        dir: &Path,
        inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    ) -> Result<u64, GfError> {
        if self.is_empty() {
            return Ok(0);
        }
        let captured;
        let inventory = if let Some(inventory) = inventory {
            inventory
        } else {
            captured = graphforge_storage::AuthenticatedPropertyInventory::capture(dir)?;
            &captured
        };
        let mut total = 0u64;
        for (stem, removals) in &self.nodes {
            total += graphforge_storage::stage_remove_node_properties_authenticated(
                staged, dir, inventory, stem, removals,
            )?;
        }
        for (stem, removals) in &self.edges {
            total += graphforge_storage::stage_remove_edge_properties_authenticated(
                staged, dir, inventory, stem, removals,
            )?;
        }
        Ok(total)
    }

    /// One staged batch across all stems, like [`SetAccumulator::apply`].
    fn apply(&self, dir: &Path) -> Result<u64, GfError> {
        let mut staged = graphforge_storage::RewriteBatch::new();
        let total = self.stage_into(&mut staged, dir, None)?;
        staged.commit_at(dir)?;
        Ok(total)
    }

    /// REMOVE analogue of [`SetAccumulator::scrub`].
    pub(super) fn scrub(&mut self, deleted: &HashSet<[u8; 16]>) {
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
    type_map: &HashMap<graphforge_value::EntityTypeId, String>,
    acc: &mut SetAccumulator,
) -> Result<(), GfError> {
    let n = batch.num_rows();
    for ((col, _), phys) in targets.iter().zip(phys_values) {
        // Evaluate the value expr once for the whole batch → a column.
        let values = phys
            .evaluate(batch)
            .and_then(|cv| cv.into_array(n))
            .map_err(GfError::from_execution_error)?;
        let id_col = batch.column(col.uuid_idx);
        for row in 0..n {
            if id_col.is_null(row) {
                continue;
            }
            let uuid =
                graphforge_core::uuid::to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
            let scalar =
                ScalarValue::try_from_array(&values, row).map_err(GfError::from_execution_error)?;
            let lit = scalar_to_ir_literal(&scalar).map_err(GfError::from_execution_error)?;
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
    type_map: &HashMap<graphforge_value::EntityTypeId, String>,
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
    type_id_to_entity_name: HashMap<graphforge_value::EntityTypeId, String>,
    mode: OntologyMode,
    dir: PathBuf,
    mutation_health: mutation::MutationHealth,
    /// Logical input schema (with `var_<n>` qualifiers) — used to build the
    /// per-target physical value exprs.
    in_df_schema: DFSchemaRef,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl GraphSetExec {
    /// Build the physical SET node from its logical counterpart and input.
    ///
    /// # Errors
    /// Rejects a write resource incompatible with the logical binding contract.
    pub fn new(
        node: &GraphSetNode,
        input: Arc<dyn ExecutionPlan>,
        resource: &write_resource::BoundWriteResource,
    ) -> Result<Self, DataFusionError> {
        resource.validate(node.write_contract.as_ref())?;
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
        Ok(Self {
            input,
            targets,
            type_id_to_entity_name: resource.type_map.clone(),
            mode: resource.mode,
            dir: resource.dir.clone(),
            mutation_health: resource.health.clone(),
            in_df_schema,
            schema,
            props,
        })
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
            mutation_health: self.mutation_health.clone(),
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

        self.mutation_health
            .check()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

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
        Ok(self
            .mutation_health
            .guard_stream(Box::pin(RecordBatchStreamAdapter::new(
                stream_schema,
                futures::stream::once(fut),
            ))))
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
    type_id_to_entity_name: HashMap<graphforge_value::EntityTypeId, String>,
    mode: OntologyMode,
    dir: PathBuf,
    mutation_health: mutation::MutationHealth,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl GraphRemoveExec {
    /// Build the physical REMOVE node from its logical counterpart and input.
    ///
    /// # Errors
    /// Rejects a write resource incompatible with the logical binding contract.
    pub fn new(
        node: &GraphRemoveNode,
        input: Arc<dyn ExecutionPlan>,
        resource: &write_resource::BoundWriteResource,
    ) -> Result<Self, DataFusionError> {
        resource.validate(node.write_contract.as_ref())?;
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
        Ok(Self {
            input,
            targets,
            type_id_to_entity_name: resource.type_map.clone(),
            mode: resource.mode,
            dir: resource.dir.clone(),
            mutation_health: resource.health.clone(),
            schema,
            props,
        })
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
            mutation_health: self.mutation_health.clone(),
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

        self.mutation_health
            .check()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

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
        Ok(self
            .mutation_health
            .guard_stream(Box::pin(RecordBatchStreamAdapter::new(
                stream_schema,
                futures::stream::once(fut),
            ))))
    }
}

#[cfg(test)]
mod tests;
