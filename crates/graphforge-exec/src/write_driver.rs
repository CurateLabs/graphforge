//! Clause-ordered write statement driver (#792, unified for single writes by
//! #817) — the plan
//! split/validation, the shared per-statement write context, and the frontier
//! extension that lets later clauses target entities created by earlier ones.
//!
//! A write statement has exactly one **read prefix** (everything before
//! the first write op), a suffix of **write ops in clause order**, and may end
//! with a terminal `RETURN` projection over the final frontier. Graph reads
//! after writes still need pending-write visibility and are rejected with a
//! clear error. The prefix runs once, its rows materialize as the [`Frontier`],
//! and each write clause consumes that same frontier — extended in place with
//! the variables CREATE clauses mint, so a later `DELETE`/`SET`/`REMOVE`
//! resolves them like any matched column.
//!
//! All effects stage during the phase loop (CREATE in the shared writer's
//! buffer, deletions in pending sets, SET/REMOVE in accumulators) and hit
//! disk in **one** [`RewriteBatch`](graphforge_storage::RewriteBatch) commit at
//! statement end — a failure in any phase aborts with the prior on-disk
//! state fully intact.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryBuilder, Int64Array, ListArray, RecordBatch,
    StringBuilder, StructArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datafusion::common::Column;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr as DfExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;

use graphforge_core::uuid::to_bytes;
use graphforge_core::{GfError, OntologyMode, TypeId};
use graphforge_ir::plan::GraphOp;
use graphforge_ir::{
    CreatePattern, Direction, ExprArena, ExprId, IrExpr, MergeSetItem, SetMapItem, SetPropItem,
    VarId,
};
use graphforge_plan::{ResolvedEdgeSpec, ResolvedNodeSpec};
use graphforge_rel::expr::scalar_to_ir_literal;
use graphforge_rel::{GraphPlanLowerer, VarMap};

use crate::{
    CreateConfig, DeleteCol, RemoveAccumulator, SetAccumulator, WriteCol, build_ref_by_var,
    collect_delete_targets, fixed_binary_uuid, validate_edge_specs, write_batch_creates,
};

// ---------------------------------------------------------------------------
// Plan splitting and validation
// ---------------------------------------------------------------------------

/// Whether `op` is a write clause (consumes the frontier, mutates the graph).
pub(crate) fn is_write_op(op: &GraphOp) -> bool {
    matches!(
        op,
        GraphOp::Create { .. }
            | GraphOp::Merge { .. }
            | GraphOp::Delete { .. }
            | GraphOp::Set { .. }
            | GraphOp::Remove { .. }
    )
}

/// A statement's clause-ordered shape: one read prefix and every write op.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SplitWritePlan {
    /// Number of leading ops forming the read prefix (may be 0 for a
    /// standalone `CREATE`).
    pub prefix_len: usize,
    /// Indices into the full op list of the write ops, in clause order.
    pub write_ops: Vec<usize>,
    /// Start index of the relational suffix after the final write, if any.
    pub read_suffix_start: Option<usize>,
}

/// Split a write statement's ops into the read prefix, clause-ordered writes,
/// and an optional relational suffix after the final write. Relational ops
/// between writes are evaluated by the driver against the shared frontier.
///
/// # Errors
/// - [`GfError::Plan`] when there is no write op (caller routed wrongly).
pub(crate) fn split_write_plan(ops: &[GraphOp]) -> Result<SplitWritePlan, GfError> {
    let Some(prefix_len) = ops.iter().position(is_write_op) else {
        return Err(GfError::Plan(
            "split_write_plan called on a plan with no write ops".into(),
        ));
    };
    let mut write_ops = Vec::new();
    for (i, op) in ops.iter().enumerate().skip(prefix_len) {
        if is_write_op(op) {
            write_ops.push(i);
        }
    }
    let last_write = *write_ops.last().expect("prefix_len points at a write op");
    let read_suffix_start = (last_write + 1 < ops.len()).then_some(last_write + 1);
    Ok(SplitWritePlan {
        prefix_len,
        write_ops,
        read_suffix_start,
    })
}

/// For a contiguous, terminal CREATE-only suffix, return the created bindings
/// each clause must retain for later clauses. Variables that are never read
/// again do not need columns in the statement frontier: their graph effects
/// are already buffered in the shared writer and will commit atomically.
///
/// Other write shapes remain conservative and materialize every binding.
pub(crate) fn create_retention_by_write(
    ops: &[GraphOp],
    exprs: &ExprArena,
    split: &SplitWritePlan,
) -> Option<HashMap<usize, HashSet<VarId>>> {
    let first_write = *split.write_ops.first()?;
    if split.read_suffix_start.is_some()
        || split.write_ops.iter().copied().ne(first_write..ops.len())
        || split
            .write_ops
            .iter()
            .any(|&index| !matches!(ops[index], GraphOp::Create { .. }))
    {
        return None;
    }

    let mut live = HashSet::new();
    let mut retention = HashMap::with_capacity(split.write_ops.len());
    for &index in split.write_ops.iter().rev() {
        retention.insert(index, live.clone());
        let GraphOp::Create { pattern } = &ops[index] else {
            unreachable!("CREATE-only shape checked above");
        };
        collect_create_inputs(pattern, exprs, &mut live);
    }
    Some(retention)
}

fn collect_create_inputs(pattern: &CreatePattern, exprs: &ExprArena, vars: &mut HashSet<VarId>) {
    for node in &pattern.nodes {
        if node.is_reference {
            vars.insert(node.var);
        }
        if let Some(properties) = node.properties {
            collect_expr_vars(exprs, properties, vars, &mut HashSet::new());
        }
    }
    for edge in &pattern.edges {
        if let Some(properties) = edge.properties {
            collect_expr_vars(exprs, properties, vars, &mut HashSet::new());
        }
    }
}

fn collect_expr_vars(
    arena: &ExprArena,
    id: ExprId,
    vars: &mut HashSet<VarId>,
    visited: &mut HashSet<ExprId>,
) {
    if !visited.insert(id) {
        return;
    }
    let mut visit = |child| collect_expr_vars(arena, child, vars, visited);
    match arena.get(id) {
        IrExpr::VarRef(var) => {
            vars.insert(*var);
        }
        IrExpr::PropertyAccess { base, .. } | IrExpr::UnaryOp { expr: base, .. } => visit(*base),
        IrExpr::BinaryOp { left, right, .. } => {
            visit(*left);
            visit(*right);
        }
        IrExpr::FunctionCall { args, .. } | IrExpr::ListLiteral(args) => {
            for child in args {
                visit(*child);
            }
        }
        IrExpr::Case {
            operand,
            arms,
            else_expr,
        } => {
            if let Some(child) = operand {
                visit(*child);
            }
            for arm in arms {
                visit(arm.when);
                visit(arm.then);
            }
            if let Some(child) = else_expr {
                visit(*child);
            }
        }
        IrExpr::MapLiteral(entries) => {
            for (_, child) in entries {
                visit(*child);
            }
        }
        IrExpr::Quantifier {
            list, predicate, ..
        } => {
            visit(*list);
            visit(*predicate);
        }
        IrExpr::ListComprehension {
            list,
            filter,
            projection,
            ..
        } => {
            visit(*list);
            if let Some(child) = filter {
                visit(*child);
            }
            if let Some(child) = projection {
                visit(*child);
            }
        }
        IrExpr::Literal(_) | IrExpr::Parameter(_) => {}
    }
}

// ---------------------------------------------------------------------------
// Statement write context
// ---------------------------------------------------------------------------

/// The openCypher write counters a statement reports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteCounters {
    pub nodes_created: u64,
    pub edges_created: u64,
    pub nodes_deleted: u64,
    pub edges_deleted: u64,
    pub properties_set: u64,
    pub properties_removed: u64,
    pub labels_added: u64,
    pub labels_removed: u64,
}

/// Mutable state shared by every write phase of one statement.
///
/// One [`GraphWriter`](graphforge_storage::GraphWriter) buffers every CREATE in the
/// statement (later phases inspect/edit it via the pending-buffer API, and a
/// single writer keeps surrogate ids collision-free across multiple CREATE
/// clauses); deletions of committed entities accumulate in the pending sets
/// for the commit phase, while deletions of pending-created entities cancel
/// in the buffer immediately.
pub(crate) struct StatementWriteContext {
    pub writer: graphforge_storage::GraphWriter,
    /// Committed nodes/edges to delete at commit.
    pub pending_node_deletes: HashSet<[u8; 16]>,
    pub pending_edge_deletes: HashSet<[u8; 16]>,
    /// Every uuid deleted so far (pending-created cancels AND committed
    /// targets): writing to or referencing one is an error (openCypher),
    /// deleting one again is a no-op.
    pub deleted: HashSet<[u8; 16]>,
    pub set_acc: SetAccumulator,
    pub remove_acc: RemoveAccumulator,
    pub label_additions: HashMap<[u8; 16], HashSet<u32>>,
    pub label_removals: HashMap<[u8; 16], HashSet<u32>>,
    /// Label tokens already present before, or introduced during, this statement.
    pub known_labels: HashSet<u32>,
    pub removed_label_tokens: HashSet<u32>,
    /// Entity/property pairs SET at least once in this statement.
    pub property_sets: HashSet<(bool, [u8; 16], String)>,
    pub counters: WriteCounters,
    mutation_effects: BTreeMap<
        crate::MutationKind,
        (
            HashSet<crate::MutationSubject>,
            HashSet<crate::MutationSubject>,
        ),
    >,
}

impl StatementWriteContext {
    /// Open the statement's shared writer on `dir`.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the writer cannot open the directory.
    pub(crate) fn new(dir: &Path, mode: OntologyMode) -> Result<Self, GfError> {
        let mut known_labels = HashSet::new();
        for batch in graphforge_storage::read_nodes(dir)
            .map_err(|error| GfError::Storage(error.to_string()))?
        {
            let Some(labels) = batch
                .column_by_name("type_ids")
                .and_then(|array| array.as_any().downcast_ref::<ListArray>())
            else {
                continue;
            };
            for row in 0..labels.len() {
                let values = labels.value(row);
                if let Some(values) = values.as_any().downcast_ref::<UInt32Array>() {
                    known_labels.extend(values.values().iter().copied());
                }
            }
        }
        Ok(Self {
            writer: graphforge_storage::GraphWriter::open(dir, mode)?,
            pending_node_deletes: HashSet::new(),
            pending_edge_deletes: HashSet::new(),
            deleted: HashSet::new(),
            set_acc: SetAccumulator::default(),
            remove_acc: RemoveAccumulator::default(),
            label_additions: HashMap::new(),
            label_removals: HashMap::new(),
            known_labels,
            removed_label_tokens: HashSet::new(),
            property_sets: HashSet::new(),
            counters: WriteCounters::default(),
            mutation_effects: BTreeMap::new(),
        })
    }

    fn record_mutation_input(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.mutation_effects
            .entry(kind)
            .or_default()
            .0
            .insert(crate::MutationSubject {
                uuid,
                kind: subject_kind,
            });
    }

    fn record_mutation_output(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.mutation_effects
            .entry(kind)
            .or_default()
            .1
            .insert(crate::MutationSubject {
                uuid,
                kind: subject_kind,
            });
    }

    pub(crate) fn mutation_receipt(&self) -> crate::MutationReceipt {
        crate::MutationReceipt::from_accumulators(self.mutation_effects.clone())
    }

    fn record_label_tokens(&mut self, labels: impl IntoIterator<Item = u32>) {
        for label in labels {
            if self.removed_label_tokens.remove(&label) {
                self.counters.labels_removed -= 1;
            }
            if self.known_labels.insert(label) {
                self.counters.labels_added += 1;
            }
        }
    }

    fn record_property_set(&mut self, is_edge: bool, uuid: [u8; 16], name: &str) -> bool {
        if self.property_sets.insert((is_edge, uuid, name.to_owned())) {
            self.counters.properties_set += 1;
            true
        } else {
            false
        }
    }

    fn record_removed_label_tokens(&mut self, labels: impl IntoIterator<Item = u32>) {
        for label in labels {
            if self.removed_label_tokens.insert(label) {
                self.counters.labels_removed += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Frontier
// ---------------------------------------------------------------------------

/// The statement's materialized read-prefix rows.
///
/// `df_schema` is the **logical** schema carrying the `var_<n>` qualifiers
/// the write phases resolve identity columns against (physical Arrow schemas
/// strip qualifiers, so column names repeat and resolution is positional via
/// the DFSchema). CREATE phases extend both in lock-step with the columns of
/// each created variable.
pub(crate) struct Frontier {
    pub df_schema: DFSchema,
    pub batches: Vec<RecordBatch>,
}

impl Frontier {
    /// Total rows across the frontier's batches.
    pub(crate) fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    fn take_rows(&mut self, indices: &[u64]) -> Result<(), GfError> {
        let schema = self
            .batches
            .first()
            .map_or_else(|| Arc::clone(self.df_schema.inner()), RecordBatch::schema);
        let input = arrow::compute::concat_batches(&schema, &self.batches)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        let indices = UInt64Array::from(indices.to_vec());
        let columns = input
            .columns()
            .iter()
            .map(|column| {
                arrow::compute::take(column, &indices, None)
                    .map_err(|error| GfError::Execution(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.batches = vec![
            RecordBatch::try_new_with_options(
                schema,
                columns,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
            )
            .map_err(|error| GfError::Execution(error.to_string()))?,
        ];
        Ok(())
    }

    /// Install one statement-local property column after SET/REMOVE so later
    /// write expressions evaluate against the accumulated statement state.
    fn overlay_property(
        &mut self,
        var: VarId,
        name: &str,
        values: Vec<ArrayRef>,
    ) -> Result<(), GfError> {
        if values.len() != self.batches.len() {
            return Err(GfError::Execution(
                "property overlay batch count does not match frontier".into(),
            ));
        }
        let data_type = values
            .first()
            .map_or(DataType::Null, |value| value.data_type().clone());
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", var.0));
        let existing = self
            .df_schema
            .index_of_column_by_name(Some(&qualifier), name);
        let mut rebuilt = Vec::with_capacity(self.batches.len());
        for (batch, value) in self.batches.iter().zip(values) {
            if value.len() != batch.num_rows() {
                return Err(GfError::Execution(format!(
                    "property overlay `{name}` has {} rows, expected {}",
                    value.len(),
                    batch.num_rows()
                )));
            }
            let mut fields = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            let mut columns = batch.columns().to_vec();
            let field = Field::new(name, value.data_type().clone(), true);
            if let Some(index) = existing {
                fields[index] = field;
                columns[index] = value;
            } else {
                fields.push(field);
                columns.push(value);
            }
            rebuilt.push(
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                    .map_err(|e| GfError::Execution(e.to_string()))?,
            );
        }
        self.batches = rebuilt;
        let mut schema_fields = self
            .df_schema
            .iter()
            .enumerate()
            .map(|(index, (relation, field))| {
                let field = if existing == Some(index) {
                    Arc::new(Field::new(name, data_type.clone(), true))
                } else {
                    Arc::clone(field)
                };
                (relation.cloned(), field)
            })
            .collect::<Vec<_>>();
        if existing.is_none() {
            schema_fields.push((Some(qualifier), Arc::new(Field::new(name, data_type, true))));
        }
        self.df_schema = DFSchema::new_with_metadata(schema_fields, HashMap::new())
            .map_err(|e| GfError::Execution(e.to_string()))?;
        Ok(())
    }

    /// Append a created **node** variable's identity columns
    /// (`var_<var>.node_uuid` / `.node_id` / `.type_id` / `.type_ids`), one value per
    /// frontier row, so later clauses can target it like a matched node.
    ///
    /// # Errors
    /// Returns [`GfError::Execution`] when the value vectors do not cover
    /// every frontier row, or on an Arrow build failure.
    #[cfg(test)]
    pub(crate) fn append_node_var(
        &mut self,
        var: u32,
        uuids: &[[u8; 16]],
        node_ids: &[u64],
        type_ids: &[u32],
    ) -> Result<(), GfError> {
        let fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("node_id", DataType::UInt64, false),
            Field::new("type_id", DataType::UInt32, false),
            Field::new(
                "type_ids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
        ];
        self.append_var_columns(var, fields, |_, range| {
            let mut uuid_b = FixedSizeBinaryBuilder::new(16);
            for u in &uuids[range.clone()] {
                uuid_b
                    .append_value(u)
                    .map_err(|e| GfError::Execution(e.to_string()))?;
            }
            Ok(vec![
                Arc::new(uuid_b.finish()) as ArrayRef,
                Arc::new(UInt64Array::from(node_ids[range.clone()].to_vec())),
                Arc::new(UInt32Array::from(type_ids[range.clone()].to_vec())),
                singleton_label_sets(&type_ids[range]),
            ])
        })
    }

    pub(crate) fn append_created_node_var(
        &mut self,
        spec: &ResolvedNodeSpec,
        uuids: &[[u8; 16]],
        node_ids: &[u64],
        type_ids: &[u32],
        computed_batches: &[crate::CreateComputed],
    ) -> Result<(), GfError> {
        let mut fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("node_id", DataType::UInt64, false),
            Field::new("type_id", DataType::UInt32, false),
            Field::new(
                "type_ids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
        ];
        for (name, lit) in &spec.properties {
            fields.push(Field::new(
                name,
                graphforge_rel::expr::ir_literal_to_scalar(lit).data_type(),
                true,
            ));
        }
        for (name, _) in &spec.computed_properties {
            let ty = computed_type(computed_batches, spec.var, name);
            fields.push(Field::new(name, ty, true));
        }

        self.append_var_columns(spec.var, fields, |batch_idx, range| {
            let rows = range.len();
            let mut uuid_b = FixedSizeBinaryBuilder::new(16);
            for u in &uuids[range.clone()] {
                uuid_b
                    .append_value(u)
                    .map_err(|e| GfError::Execution(e.to_string()))?;
            }
            let mut cols: Vec<ArrayRef> = vec![
                Arc::new(uuid_b.finish()) as ArrayRef,
                Arc::new(UInt64Array::from(node_ids[range.clone()].to_vec())),
                Arc::new(UInt32Array::from(type_ids[range].to_vec())),
                repeated_label_sets(&spec.label_ids, rows),
            ];
            for (_, lit) in &spec.properties {
                let scalar = graphforge_rel::expr::ir_literal_to_scalar(lit);
                cols.push(
                    scalar
                        .to_array_of_size(rows)
                        .map_err(|e| GfError::Execution(e.to_string()))?,
                );
            }
            for (name, _) in &spec.computed_properties {
                cols.push(computed_array(
                    computed_batches,
                    batch_idx,
                    spec.var,
                    name,
                    rows,
                )?);
            }
            Ok(cols)
        })
    }

    fn append_merged_node_rows(
        &mut self,
        var: u32,
        rows: &[MatchedMergeNode],
    ) -> Result<(), GfError> {
        let mut property_types = HashMap::new();
        for row in rows {
            for (name, value) in &row.properties {
                let data_type = graphforge_rel::expr::ir_literal_to_scalar(value).data_type();
                if let Some(existing) = property_types.get(name) {
                    if existing != &data_type {
                        return Err(GfError::Execution(format!(
                            "MERGE property `{name}` has incompatible row types {existing:?} and {data_type:?}"
                        )));
                    }
                } else {
                    property_types.insert(name.clone(), data_type);
                }
            }
        }
        let mut properties = property_types.into_iter().collect::<Vec<_>>();
        properties.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("node_id", DataType::UInt64, false),
            Field::new("type_id", DataType::UInt32, false),
            Field::new(
                "type_ids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
        ];
        fields.extend(
            properties
                .iter()
                .map(|(name, data_type)| Field::new(name, data_type.clone(), true)),
        );

        self.append_var_columns(var, fields, |_, range| {
            let selected = &rows[range];
            let mut uuid_builder = FixedSizeBinaryBuilder::new(16);
            for row in selected {
                uuid_builder
                    .append_value(row.uuid)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
            }
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(uuid_builder.finish()),
                Arc::new(UInt64Array::from(
                    selected.iter().map(|row| row.node_id).collect::<Vec<_>>(),
                )),
                Arc::new(UInt32Array::from(
                    selected.iter().map(|row| row.type_id).collect::<Vec<_>>(),
                )),
                repeated_row_label_sets(selected),
            ];
            for (name, data_type) in &properties {
                let values = selected.iter().map(|row| {
                    row.properties.get(name).map_or_else(
                        || ScalarValue::try_new_null(data_type),
                        |value| Ok(graphforge_rel::expr::ir_literal_to_scalar(value)),
                    )
                });
                let values = values
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                columns.push(
                    ScalarValue::iter_to_array(values)
                        .map_err(|error| GfError::Execution(error.to_string()))?,
                );
            }
            Ok(columns)
        })
    }

    fn rename_unqualified_collisions(
        &mut self,
        names: &HashSet<String>,
        var_map: &mut VarMap,
    ) -> Result<(), GfError> {
        let renames = self
            .df_schema
            .iter()
            .enumerate()
            .filter(|(_, (qualifier, field))| qualifier.is_none() && names.contains(field.name()))
            .map(|(index, (_, field))| {
                (index, field.name().clone(), format!("__gf_scalar_{index}"))
            })
            .collect::<Vec<_>>();
        if renames.is_empty() {
            return Ok(());
        }
        let mut logical_fields = self
            .df_schema
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect::<Vec<_>>();
        for (index, _, replacement) in &renames {
            logical_fields[*index].1 = Arc::new(
                logical_fields[*index]
                    .1
                    .as_ref()
                    .clone()
                    .with_name(replacement),
            );
        }
        self.df_schema = DFSchema::new_with_metadata(logical_fields, HashMap::new())
            .map_err(|error| GfError::Execution(error.to_string()))?;

        self.batches = self
            .batches
            .iter()
            .map(|batch| {
                let mut fields = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect::<Vec<_>>();
                for (index, _, replacement) in &renames {
                    fields[*index] = fields[*index].clone().with_name(replacement);
                }
                RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
                    .map_err(|error| GfError::Execution(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mappings = var_map
            .var_ids()
            .filter_map(|var| var_map.get(var).map(|name| (var, name.to_owned())))
            .collect::<Vec<_>>();
        for (var, current) in mappings {
            if let Some((_, _, replacement)) =
                renames.iter().find(|(_, original, _)| original == &current)
            {
                var_map.insert(var, replacement.clone());
            }
        }
        Ok(())
    }

    /// Append a created **edge** variable's identity columns
    /// (`var_<var>.edge_uuid` / `.rel_type_name`), one value per frontier row.
    ///
    /// # Errors
    /// Returns [`GfError::Execution`] when the value vectors do not cover
    /// every frontier row, or on an Arrow build failure.
    #[cfg(test)]
    pub(crate) fn append_edge_var(
        &mut self,
        var: u32,
        uuids: &[[u8; 16]],
        src_uuids: &[[u8; 16]],
        dst_uuids: &[[u8; 16]],
        rel_names: &[Option<String>],
    ) -> Result<(), GfError> {
        let fields = vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("rel_type_name", DataType::Utf8, true),
        ];
        self.append_var_columns(var, fields, |_, range| {
            let mut uuid_b = FixedSizeBinaryBuilder::new(16);
            let mut src_b = FixedSizeBinaryBuilder::new(16);
            let mut dst_b = FixedSizeBinaryBuilder::new(16);
            let mut name_b = StringBuilder::new();
            for row in range {
                uuid_b
                    .append_value(uuids[row])
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                src_b
                    .append_value(src_uuids[row])
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                dst_b
                    .append_value(dst_uuids[row])
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                name_b.append_option(rel_names[row].as_deref());
            }
            Ok(vec![
                Arc::new(uuid_b.finish()) as ArrayRef,
                Arc::new(src_b.finish()),
                Arc::new(dst_b.finish()),
                Arc::new(name_b.finish()),
            ])
        })
    }

    fn append_created_edge_var(
        &mut self,
        spec: &ResolvedEdgeSpec,
        identities: &EdgeIdentities,
        computed_batches: &[crate::CreateComputed],
    ) -> Result<(), GfError> {
        let mut fields = vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("rel_type_name", DataType::Utf8, true),
        ];
        for (name, lit) in &spec.properties {
            fields.push(Field::new(
                name,
                graphforge_rel::expr::ir_literal_to_scalar(lit).data_type(),
                true,
            ));
        }
        for (name, _) in &spec.computed_properties {
            fields.push(Field::new(
                name,
                computed_type(computed_batches, spec.var, name),
                true,
            ));
        }
        self.append_var_columns(spec.var, fields, |batch_idx, range| {
            let rows = range.len();
            let mut uuid_b = FixedSizeBinaryBuilder::new(16);
            let mut src_b = FixedSizeBinaryBuilder::new(16);
            let mut dst_b = FixedSizeBinaryBuilder::new(16);
            let mut name_b = StringBuilder::new();
            for row in range.clone() {
                uuid_b
                    .append_value(identities.uuids[row])
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                src_b
                    .append_value(identities.src_uuids[row])
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                dst_b
                    .append_value(identities.dst_uuids[row])
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                name_b.append_option(identities.rel_names[row].as_deref());
            }
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(uuid_b.finish()),
                Arc::new(src_b.finish()),
                Arc::new(dst_b.finish()),
                Arc::new(name_b.finish()),
            ];
            for (_, lit) in &spec.properties {
                columns.push(
                    graphforge_rel::expr::ir_literal_to_scalar(lit)
                        .to_array_of_size(rows)
                        .map_err(|e| GfError::Execution(e.to_string()))?,
                );
            }
            for (name, _) in &spec.computed_properties {
                columns.push(computed_array(
                    computed_batches,
                    batch_idx,
                    spec.var,
                    name,
                    rows,
                )?);
            }
            Ok(columns)
        })
    }

    fn append_merged_edge_rows(
        &mut self,
        var: u32,
        rows: &[MatchedMergeEdge],
    ) -> Result<(), GfError> {
        let mut property_types = HashMap::new();
        for row in rows {
            for (name, value) in &row.properties {
                property_types.entry(name.clone()).or_insert_with(|| {
                    graphforge_rel::expr::ir_literal_to_scalar(value).data_type()
                });
            }
        }
        let mut properties = property_types.into_iter().collect::<Vec<_>>();
        properties.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut fields = vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("rel_type_name", DataType::Utf8, true),
        ];
        fields.extend(
            properties
                .iter()
                .map(|(name, data_type)| Field::new(name, data_type.clone(), true)),
        );
        self.append_var_columns(var, fields, |_, range| {
            let selected = &rows[range];
            let mut edge_builder = FixedSizeBinaryBuilder::new(16);
            let mut src_builder = FixedSizeBinaryBuilder::new(16);
            let mut dst_builder = FixedSizeBinaryBuilder::new(16);
            let mut type_builder = StringBuilder::new();
            for row in selected {
                edge_builder
                    .append_value(row.uuid)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                src_builder
                    .append_value(row.src_uuid)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                dst_builder
                    .append_value(row.dst_uuid)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                type_builder.append_value(&row.rel_type);
            }
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(edge_builder.finish()),
                Arc::new(src_builder.finish()),
                Arc::new(dst_builder.finish()),
                Arc::new(type_builder.finish()),
            ];
            for (name, data_type) in &properties {
                let values = selected
                    .iter()
                    .map(|row| {
                        row.properties.get(name).map_or_else(
                            || ScalarValue::try_new_null(data_type),
                            |value| Ok(graphforge_rel::expr::ir_literal_to_scalar(value)),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                columns.push(
                    ScalarValue::iter_to_array(values)
                        .map_err(|error| GfError::Execution(error.to_string()))?,
                );
            }
            Ok(columns)
        })
    }

    fn add_node_labels(
        &mut self,
        var: VarId,
        labels: &[u32],
        mask: &[bool],
    ) -> Result<(), GfError> {
        if mask.len() != self.num_rows() {
            return Err(GfError::Execution(
                "MERGE label mask does not match frontier rows".into(),
            ));
        }
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", var.0));
        let index = self
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan("MERGE label target has no type_ids column".into()))?;
        let mut rebuilt = Vec::with_capacity(self.batches.len());
        let mut offset = 0usize;
        for batch in &self.batches {
            let current = batch
                .column(index)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Execution("node type_ids are not a list".into()))?;
            let mut rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let values = current.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not UInt32".into()))?;
                let mut merged = values.values().to_vec();
                if mask[offset + row] {
                    merged.extend_from_slice(labels);
                    merged.sort_unstable();
                    merged.dedup();
                }
                rows.push(Some(merged.into_iter().map(Some).collect::<Vec<_>>()));
            }
            let nullable = ListArray::from_iter_primitive::<UInt32Type, _, _>(rows);
            let list = ListArray::new(
                Arc::new(Field::new("item", DataType::UInt32, false)),
                nullable.offsets().clone(),
                nullable.values().clone(),
                nullable.nulls().cloned(),
            );
            let mut columns = batch.columns().to_vec();
            columns[index] = Arc::new(list);
            rebuilt.push(
                RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|e| GfError::Execution(e.to_string()))?,
            );
            offset += batch.num_rows();
        }
        self.batches = rebuilt;
        Ok(())
    }

    fn remove_node_labels(&mut self, var: VarId, labels: &[u32]) -> Result<(), GfError> {
        let mask = vec![true; self.num_rows()];
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", var.0));
        let index = self
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan("REMOVE label target has no type_ids column".into()))?;
        let mut rebuilt = Vec::with_capacity(self.batches.len());
        let mut offset = 0usize;
        for batch in &self.batches {
            let current = batch
                .column(index)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Execution("node type_ids are not a list".into()))?;
            let mut rows = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let values = current.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not UInt32".into()))?;
                let mut retained = values.values().to_vec();
                if mask[offset + row] {
                    retained.retain(|label| !labels.contains(label));
                }
                rows.push(Some(retained.into_iter().map(Some).collect::<Vec<_>>()));
            }
            let nullable = ListArray::from_iter_primitive::<UInt32Type, _, _>(rows);
            let list = ListArray::new(
                Arc::new(Field::new("item", DataType::UInt32, false)),
                nullable.offsets().clone(),
                nullable.values().clone(),
                nullable.nulls().cloned(),
            );
            let mut columns = batch.columns().to_vec();
            columns[index] = Arc::new(list);
            rebuilt.push(
                RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|e| GfError::Execution(e.to_string()))?,
            );
            offset += batch.num_rows();
        }
        self.batches = rebuilt;
        Ok(())
    }

    /// Shared core of the `append_*_var` methods: per batch, build the new
    /// columns for that batch's row range and rebuild the batch; then extend
    /// the logical schema with the `var_<var>`-qualified fields (positions
    /// stay aligned because both sides append in the same order).
    fn append_var_columns(
        &mut self,
        var: u32,
        fields: Vec<Field>,
        mut build: impl FnMut(usize, std::ops::Range<usize>) -> Result<Vec<ArrayRef>, GfError>,
    ) -> Result<(), GfError> {
        let exec_err = |m: String| GfError::Execution(m);

        let mut offset = 0usize;
        let mut new_batches = Vec::with_capacity(self.batches.len());
        for (batch_idx, batch) in self.batches.iter().enumerate() {
            let n = batch.num_rows();
            let new_cols = build(batch_idx, offset..offset + n)?;
            offset += n;

            let mut schema_fields: Vec<Field> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            schema_fields.extend(fields.iter().cloned());
            let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
            cols.extend(new_cols);
            new_batches.push(
                RecordBatch::try_new(Arc::new(Schema::new(schema_fields)), cols)
                    .map_err(|e| exec_err(e.to_string()))?,
            );
        }
        self.batches = new_batches;

        let added = DFSchema::try_from_qualified_schema(format!("var_{var}"), &Schema::new(fields))
            .map_err(|e| exec_err(e.to_string()))?;
        let qualified = self
            .df_schema
            .iter()
            .chain(added.iter())
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect();
        self.df_schema = DFSchema::new_with_metadata(qualified, HashMap::new())
            .map_err(|e| exec_err(e.to_string()))?;
        Ok(())
    }
}

pub(crate) fn repeated_label_sets(labels: &[u32], rows: usize) -> ArrayRef {
    let array = ListArray::from_iter_primitive::<UInt32Type, _, _>(
        (0..rows).map(|_| Some(labels.iter().copied().map(Some))),
    );
    non_null_label_items(&array)
}

fn repeated_row_label_sets(rows: &[MatchedMergeNode]) -> ArrayRef {
    let array = ListArray::from_iter_primitive::<UInt32Type, _, _>(
        rows.iter()
            .map(|row| Some(row.label_ids.iter().copied().map(Some))),
    );
    non_null_label_items(&array)
}

#[cfg(test)]
fn singleton_label_sets(labels: &[u32]) -> ArrayRef {
    let array = ListArray::from_iter_primitive::<UInt32Type, _, _>(
        labels.iter().map(|label| Some([Some(*label)])),
    );
    non_null_label_items(&array)
}

fn non_null_label_items(array: &ListArray) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        array.offsets().clone(),
        array.values().clone(),
        None,
    ))
}

fn computed_type(computed_batches: &[crate::CreateComputed], var: u32, name: &str) -> DataType {
    for computed in computed_batches {
        if let Some(cols) = computed.get(&var)
            && let Some((_, array)) = cols.iter().find(|(col_name, _)| col_name == name)
        {
            return array.data_type().clone();
        }
    }
    DataType::Null
}

fn computed_array(
    computed_batches: &[crate::CreateComputed],
    batch_idx: usize,
    var: u32,
    name: &str,
    rows: usize,
) -> Result<ArrayRef, GfError> {
    let Some(cols) = computed_batches.get(batch_idx).and_then(|m| m.get(&var)) else {
        return Err(GfError::Execution(format!(
            "computed property {name} for var {var} was not evaluated"
        )));
    };
    let Some((_, array)) = cols.iter().find(|(col_name, _)| col_name == name) else {
        return Err(GfError::Execution(format!(
            "computed property {name} for var {var} was not evaluated"
        )));
    };
    if array.len() != rows {
        return Err(GfError::Execution(format!(
            "computed property {name} for var {var} has {} rows, expected {rows}",
            array.len()
        )));
    }
    Ok(Arc::clone(array))
}

// ---------------------------------------------------------------------------
// Create recorder
// ---------------------------------------------------------------------------

/// One created node variable's per-row identities, in frontier row order.
#[derive(Default)]
struct NodeIdentities {
    uuids: Vec<[u8; 16]>,
    node_ids: Vec<u64>,
    type_ids: Vec<u32>,
}

type NodeIdentitySlices<'a> = (&'a [[u8; 16]], &'a [u64], &'a [u32]);

/// One created edge variable's per-row identities, in frontier row order.
#[derive(Default)]
struct EdgeIdentities {
    uuids: Vec<[u8; 16]>,
    src_uuids: Vec<[u8; 16]>,
    dst_uuids: Vec<[u8; 16]>,
    rel_names: Vec<Option<String>>,
}

/// Per-variable identities minted by one CREATE phase, one entry per frontier
/// row, recorded by `write_batch_creates` so the driver can extend the
/// [`Frontier`] (and the `VarMap`) with the created variables.
#[derive(Default)]
pub(crate) struct CreateRecorder {
    nodes: HashMap<u32, NodeIdentities>,
    edges: HashMap<u32, EdgeIdentities>,
}

impl CreateRecorder {
    pub(crate) fn record_node(&mut self, var: u32, uuid: [u8; 16], node_id: u64, type_id: u32) {
        let n = self.nodes.entry(var).or_default();
        n.uuids.push(uuid);
        n.node_ids.push(node_id);
        n.type_ids.push(type_id);
    }

    pub(crate) fn record_edge(
        &mut self,
        var: u32,
        uuid: [u8; 16],
        src_uuid: [u8; 16],
        dst_uuid: [u8; 16],
        rel_name: Option<String>,
    ) {
        let e = self.edges.entry(var).or_default();
        e.uuids.push(uuid);
        e.src_uuids.push(src_uuid);
        e.dst_uuids.push(dst_uuid);
        e.rel_names.push(rel_name);
    }

    /// Borrow the identities minted for one created node var, in frontier row
    /// order. Emit-rows CREATE uses this to build its result relation while
    /// sharing the same write path as the statement driver (#814).
    pub(crate) fn node_identities(&self, var: u32) -> Option<NodeIdentitySlices<'_>> {
        self.nodes.get(&var).map(|n| {
            (
                n.uuids.as_slice(),
                n.node_ids.as_slice(),
                n.type_ids.as_slice(),
            )
        })
    }

    fn record_create_receipt(&self, ctx: &mut StatementWriteContext) {
        for node in self.nodes.values() {
            for uuid in &node.uuids {
                ctx.record_mutation_output(
                    crate::MutationKind::CreateNode,
                    crate::MutationSubjectKind::Node,
                    *uuid,
                );
            }
        }
        for edge in self.edges.values() {
            for ((uuid, src_uuid), dst_uuid) in
                edge.uuids.iter().zip(&edge.src_uuids).zip(&edge.dst_uuids)
            {
                ctx.record_mutation_input(
                    crate::MutationKind::CreateEdge,
                    crate::MutationSubjectKind::Node,
                    *src_uuid,
                );
                ctx.record_mutation_input(
                    crate::MutationKind::CreateEdge,
                    crate::MutationSubjectKind::Node,
                    *dst_uuid,
                );
                ctx.record_mutation_output(
                    crate::MutationKind::CreateEdge,
                    crate::MutationSubjectKind::Edge,
                    *uuid,
                );
            }
        }
    }

    /// Extend `frontier` and `var_map` with every recorded variable so later
    /// clauses resolve the created entities like matched ones. Each variable
    /// must have exactly one identity per frontier row (the create runs once
    /// per row).
    fn extend_frontier(
        mut self,
        frontier: &mut Frontier,
        var_map: &mut VarMap,
        node_specs: &[ResolvedNodeSpec],
        edge_specs: &[ResolvedEdgeSpec],
        computed_batches: &[crate::CreateComputed],
        retain_created: Option<&HashSet<VarId>>,
    ) -> Result<(), GfError> {
        let rows = frontier.num_rows();
        for spec in node_specs.iter().filter(|node| {
            !node.is_reference
                && retain_created.is_none_or(|retain| retain.contains(&VarId(node.var)))
        }) {
            let var = spec.var;
            let n = self.nodes.remove(&var).unwrap_or_default();
            if n.uuids.len() != rows {
                return Err(GfError::Execution(format!(
                    "created var {var} has {} identities for {rows} rows",
                    n.uuids.len()
                )));
            }
            frontier.append_created_node_var(
                spec,
                &n.uuids,
                &n.node_ids,
                &n.type_ids,
                computed_batches,
            )?;
            var_map.insert(VarId(var), format!("var_{var}"));
        }
        for (var, e) in self.edges {
            if retain_created.is_some_and(|retain| !retain.contains(&VarId(var))) {
                continue;
            }
            if e.uuids.len() != rows {
                return Err(GfError::Execution(format!(
                    "created edge var {var} has {} identities for {rows} rows",
                    e.uuids.len()
                )));
            }
            let spec = edge_specs
                .iter()
                .find(|spec| spec.var == var)
                .ok_or_else(|| GfError::Plan(format!("created edge var {var} has no spec")))?;
            frontier.append_created_edge_var(spec, &e, computed_batches)?;
            var_map.insert(VarId(var), format!("var_{var}"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Phase loop
// ---------------------------------------------------------------------------

/// Everything the phases borrow from the session.
pub(crate) struct PhaseEnv<'a> {
    pub lowerer: &'a GraphPlanLowerer<'a>,
    pub exprs: &'a ExprArena,
    pub dir: &'a Path,
    pub mode: OntologyMode,
    pub params: &'a HashMap<String, graphforge_ir::IrLiteral>,
    /// `TypeId.0 → entity name`, for property-file stem resolution.
    pub type_map: HashMap<u32, String>,
}

fn bind_expr_params(
    expr: DfExpr,
    params: &HashMap<String, graphforge_ir::IrLiteral>,
) -> Result<DfExpr, GfError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    expr.transform_up(|expr| {
        let DfExpr::Placeholder(placeholder) = &expr else {
            return Ok(Transformed::no(expr));
        };
        let name = placeholder.id.strip_prefix('$').unwrap_or(&placeholder.id);
        Ok(params.get(name).map_or_else(
            || Transformed::no(expr),
            |value| {
                Transformed::yes(DfExpr::Literal(
                    graphforge_rel::expr::ir_literal_to_scalar(value),
                    None,
                ))
            },
        ))
    })
    .map(|transformed| transformed.data)
    .map_err(|e| GfError::Plan(e.to_string()))
}

/// Run the statement's write ops in clause order against the shared frontier
/// and context. Nothing touches disk here — effects buffer in `ctx` and
/// commit in [`commit_statement`].
pub(crate) fn run_write_phases(
    env: &PhaseEnv<'_>,
    ops: &[GraphOp],
    write_ops: &[usize],
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
    retain_created: Option<&HashSet<VarId>>,
) -> Result<(), GfError> {
    for &i in write_ops {
        match &ops[i] {
            GraphOp::Create { pattern } => {
                run_create_phase(env, pattern, frontier, var_map, ctx, retain_created)?;
            }
            GraphOp::Merge {
                pattern,
                on_create,
                on_match,
            } => run_merge_phase(env, pattern, on_create, on_match, frontier, var_map, ctx)?,
            GraphOp::Delete {
                vars,
                exprs,
                detach,
            } => {
                run_delete_phase(env, vars, exprs, *detach, frontier, var_map, ctx)?;
            }
            GraphOp::Set {
                items,
                map_items,
                label_items,
            } => {
                run_set_phase(env, items, frontier, var_map, ctx)?;
                run_set_map_phase(env, map_items, frontier, var_map, ctx)?;
                run_label_phase(label_items, true, frontier, ctx)?;
            }
            GraphOp::Remove { items, label_items } => {
                run_remove_phase(env, items, frontier, ctx)?;
                run_label_phase(label_items, false, frontier, ctx)?;
            }
            other => {
                return Err(GfError::Plan(format!(
                    "unsupported write op in the statement driver: {other:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Execute a validated terminal relational suffix over the final frontier.
/// Its synthetic empty leaf is replaced with the materialized frontier, so no
/// graph storage is reread before the statement commits.
pub(crate) async fn run_terminal_suffix(
    session: &datafusion::execution::context::SessionContext,
    logical: &datafusion::logical_expr::LogicalPlan,
    frontier: &Frontier,
) -> Result<(SchemaRef, Vec<RecordBatch>, u64), GfError> {
    if matches!(logical, datafusion::logical_expr::LogicalPlan::Limit(limit)
        if matches!(limit.fetch.as_deref(),
            Some(DfExpr::Literal(
                ScalarValue::UInt64(Some(0))
                | ScalarValue::Int64(Some(0))
                | ScalarValue::UInt32(Some(0))
                | ScalarValue::Int32(Some(0)),
                _
            )))
    ) {
        let schema = Arc::clone(logical.schema().inner());
        return Ok((Arc::clone(&schema), vec![RecordBatch::new_empty(schema)], 0));
    }
    if let Some(batch) = terminal_global_count(logical, frontier)? {
        return Ok((batch.schema(), vec![batch], 1));
    }
    let physical = session
        .state()
        .create_physical_plan(logical)
        .await
        .map_err(|e| GfError::Plan(e.to_string()))?;
    let (input_schema, input_batches) = terminal_input(frontier);
    let input = MemorySourceConfig::try_new_from_batches(input_schema, input_batches)
        .map_err(|e| GfError::Plan(e.to_string()))?;
    let physical = replace_empty_input(physical, input)?;
    let schema = physical.schema();
    let mut batches = datafusion::physical_plan::collect(physical, session.task_ctx())
        .await
        .map_err(|e| GfError::Execution(e.to_string()))?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(Arc::clone(&schema)));
    }
    let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok((schema, batches, rows))
}

fn terminal_global_count(
    logical: &datafusion::logical_expr::LogicalPlan,
    frontier: &Frontier,
) -> Result<Option<RecordBatch>, GfError> {
    let datafusion::logical_expr::LogicalPlan::Aggregate(aggregate) = logical else {
        return Ok(None);
    };
    if !aggregate.group_expr.is_empty() {
        return Ok(None);
    }
    let mut counts = Vec::with_capacity(aggregate.aggr_expr.len());
    for expr in &aggregate.aggr_expr {
        let mut expr = expr;
        while let DfExpr::Alias(alias) = expr {
            expr = alias.expr.as_ref();
        }
        let DfExpr::AggregateFunction(function) = expr else {
            return Ok(None);
        };
        if function.func.name() != "count" || function.params.distinct {
            return Ok(None);
        }
        let count = match function.params.args.as_slice() {
            [DfExpr::Literal(_, _)] | [] => i64::try_from(frontier.num_rows())
                .map_err(|_| GfError::Execution("COUNT result exceeds Int64".into()))?,
            [DfExpr::Column(column)] => {
                let index = frontier
                    .df_schema
                    .index_of_column(column)
                    .map_err(|error| GfError::Plan(error.to_string()))?;
                frontier
                    .batches
                    .iter()
                    .map(|batch| {
                        i64::try_from(batch.num_rows() - batch.column(index).null_count())
                            .map_err(|_| GfError::Execution("COUNT result exceeds Int64".into()))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .sum()
            }
            _ => return Ok(None),
        };
        counts.push(Arc::new(Int64Array::from(vec![count])) as ArrayRef);
    }
    let schema = Arc::clone(logical.schema().inner());
    RecordBatch::try_new(schema, counts)
        .map(Some)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn terminal_input(frontier: &Frontier) -> (SchemaRef, Vec<RecordBatch>) {
    if let Some(first) = frontier.batches.first() {
        (first.schema(), frontier.batches.clone())
    } else {
        let schema = Arc::clone(frontier.df_schema.inner());
        (Arc::clone(&schema), vec![RecordBatch::new_empty(schema)])
    }
}

fn replace_empty_input(
    plan: Arc<dyn ExecutionPlan>,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>, GfError> {
    if plan.as_any().is::<EmptyExec>() || plan.as_any().is::<PlaceholderRowExec>() {
        return Ok(input);
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| replace_empty_input(Arc::clone(child), Arc::clone(&input)))
        .collect::<Result<Vec<_>, _>>()?;
    plan.with_new_children(children)
        .map_err(|e| GfError::Plan(e.to_string()))
}

/// CREATE phase: mint per frontier row into the shared writer, then extend
/// the frontier with the created variables.
fn run_create_phase(
    env: &PhaseEnv<'_>,
    pattern: &CreatePattern,
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
    retain_created: Option<&HashSet<VarId>>,
) -> Result<(), GfError> {
    let (mut nodes, mut edges) = env.lowerer.resolve_create_pattern(
        pattern,
        env.exprs,
        var_map,
        &Arc::new(frontier.df_schema.clone()),
    )?;
    for name in nodes.iter().flat_map(|node| {
        node.properties
            .iter()
            .map(|(name, _)| name)
            .chain(node.computed_properties.iter().map(|(name, _)| name))
    }) {
        if matches!(name.as_str(), "node_uuid" | "node_id" | "type_id") {
            return Err(GfError::Plan(format!(
                "CREATE property `{name}` collides with a reserved node topology field"
            )));
        }
    }
    for (_, expr) in nodes
        .iter_mut()
        .flat_map(|node| node.computed_properties.iter_mut())
        .chain(
            edges
                .iter_mut()
                .flat_map(|edge| edge.computed_properties.iter_mut()),
        )
    {
        *expr = bind_expr_params(expr.clone(), env.params)?;
    }
    env.lowerer.register_created_node_shapes(&nodes);
    let cfg = CreateConfig {
        ref_cols: nodes
            .iter()
            .filter(|n| n.is_reference)
            .filter_map(|n| {
                let alias = var_map
                    .get(VarId(n.var))
                    .map_or_else(|| format!("var_{}", n.var), ToString::to_string);
                crate::RefNodeCols::resolve_with_alias(&frontier.df_schema, n.var, &alias)
            })
            .collect(),
        nodes,
        edges,
        in_df_schema: Arc::new(frontier.df_schema.clone()),
        dir: env.dir.to_path_buf(),
        mode: env.mode,
        out_schema: graphforge_plan::GraphCreateNode::summary_schema(),
    };
    validate_edge_specs(&cfg)?;
    let ref_by_var = build_ref_by_var(&cfg);

    let mut recorder = CreateRecorder::default();
    let mut tally = crate::CreateTally::default();
    let mut computed_batches = Vec::with_capacity(frontier.batches.len());
    for batch in &frontier.batches {
        // Evaluate any row-dependent property values against this batch (#814).
        let computed = crate::eval_create_computed(&cfg, batch)
            .map_err(|e| GfError::Execution(e.to_string()))?;
        write_batch_creates(
            &cfg,
            &mut ctx.writer,
            batch,
            &ref_by_var,
            crate::CreateExtras {
                deleted: Some(&ctx.deleted),
                recorder: Some(&mut recorder),
                computed: Some(&computed),
                persisted_ids: None,
            },
            &mut tally,
        )?;
        computed_batches.push(computed);
    }
    // Fold the CREATE phase tallies into the statement's write ledger.
    if tally.nodes_created > 0 {
        ctx.record_label_tokens(
            cfg.nodes
                .iter()
                .filter(|node| !node.is_reference)
                .flat_map(|node| node.label_ids.iter().copied()),
        );
    }
    ctx.counters.nodes_created += tally.nodes_created;
    ctx.counters.edges_created += tally.edges_created;
    ctx.counters.properties_set += tally.properties_set;
    recorder.record_create_receipt(ctx);
    recorder.extend_frontier(
        frontier,
        var_map,
        &cfg.nodes,
        &cfg.edges,
        &computed_batches,
        retain_created,
    )
}

fn run_merge_phase(
    env: &PhaseEnv<'_>,
    pattern: &CreatePattern,
    on_create: &[MergeSetItem],
    on_match: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let (nodes, edges) = env.lowerer.resolve_create_pattern(
        pattern,
        env.exprs,
        var_map,
        &Arc::new(frontier.df_schema.clone()),
    )?;
    env.lowerer.register_created_node_shapes(&nodes);
    if edges.len() == 1 && nodes.iter().all(|node| node.is_reference) {
        return run_relationship_merge_phase(
            env, &edges[0], on_create, on_match, frontier, var_map, ctx,
        );
    }
    if nodes.len() != 1 || !edges.is_empty() || nodes[0].is_reference {
        return Err(GfError::Plan(
            "relationship and multi-node MERGE execution is not implemented yet".into(),
        ));
    }
    let specs = resolve_merge_node_properties_by_row(env, &nodes[0], frontier)?;
    let mut merged = Vec::new();
    let mut created = Vec::new();
    let mut source_rows = Vec::new();
    for (source_row, spec) in specs.iter().enumerate() {
        reject_null_merge_properties(&spec.properties)?;
        let found = find_matching_merge_nodes(env, &ctx.writer, spec, &ctx.deleted)?;
        if found.is_empty() {
            merged.push(create_single_merge_node(spec, ctx)?);
            created.push(true);
            source_rows.push(source_row as u64);
        } else {
            for node in found {
                merged.push(node);
                created.push(false);
                source_rows.push(source_row as u64);
            }
        }
    }
    frontier.take_rows(&source_rows)?;
    let property_names = merged
        .iter()
        .flat_map(|row| row.properties.keys().cloned())
        .collect::<HashSet<_>>();
    frontier.rename_unqualified_collisions(&property_names, var_map)?;
    frontier.append_merged_node_rows(nodes[0].var, &merged)?;
    var_map.insert(VarId(nodes[0].var), format!("var_{}", nodes[0].var));
    for (row, was_created) in merged.iter().zip(&created) {
        if *was_created {
            ctx.record_mutation_output(
                crate::MutationKind::MergeCreate,
                crate::MutationSubjectKind::Node,
                row.uuid,
            );
        } else {
            ctx.record_mutation_input(
                crate::MutationKind::MergeMatchedNoop,
                crate::MutationSubjectKind::Node,
                row.uuid,
            );
        }
    }

    let any_created = created.iter().any(|value| *value);
    let any_matched = created.iter().any(|value| !*value);
    match (any_created, any_matched) {
        (true, false) => run_merge_actions(env, on_create, frontier, var_map, ctx)?,
        (false, true) => run_merge_actions(env, on_match, frontier, var_map, ctx)?,
        (true, true) => {
            run_merge_actions_masked(env, on_create, frontier, var_map, ctx, &created)?;
            let matched = created.iter().map(|value| !value).collect::<Vec<_>>();
            run_merge_actions_masked(env, on_match, frontier, var_map, ctx, &matched)?;
        }
        (false, false) => {}
    }
    Ok(())
}

fn create_single_merge_node(
    spec: &ResolvedNodeSpec,
    ctx: &mut StatementWriteContext,
) -> Result<MatchedMergeNode, GfError> {
    let uuid = graphforge_core::uuid::new_v7();
    let labels = spec
        .label_ids
        .iter()
        .copied()
        .map(TypeId)
        .collect::<Vec<_>>();
    let node_id = ctx.writer.create_node_with_labels(uuid, &labels)?;
    ctx.writer.set_properties(
        &uuid,
        spec.label_names.first().map(String::as_str),
        spec.properties.iter().cloned().collect(),
    )?;
    ctx.counters.nodes_created += 1;
    ctx.counters.properties_set += spec.properties.len() as u64;
    ctx.record_label_tokens(spec.label_ids.iter().copied());
    let type_id = spec.label_ids.first().copied().unwrap_or(u32::MAX);
    Ok(MatchedMergeNode {
        uuid: to_bytes(&uuid),
        node_id,
        type_id,
        label_ids: spec.label_ids.clone(),
        properties: spec.properties.iter().cloned().collect(),
    })
}

fn resolve_merge_node_properties_by_row(
    env: &PhaseEnv<'_>,
    spec: &ResolvedNodeSpec,
    frontier: &Frontier,
) -> Result<Vec<ResolvedNodeSpec>, GfError> {
    let mut physical = Vec::with_capacity(spec.computed_properties.len());
    for (name, expr) in &spec.computed_properties {
        let expr = bind_expr_params(expr.clone(), env.params)?;
        let (expr, eval_schema) = positional_eval_expr(expr, &frontier.df_schema)?;
        let expr = create_physical_expr(&expr, &eval_schema, &ExecutionProps::new())
            .map_err(|error| GfError::Plan(error.to_string()))?;
        physical.push((name, expr));
    }
    let mut resolved_rows = Vec::with_capacity(frontier.num_rows());
    for batch in &frontier.batches {
        let evaluated = physical
            .iter()
            .map(|(name, expr)| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
                    .map(|values| ((*name).clone(), values))
                    .map_err(|error| GfError::Execution(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let mut resolved = spec.clone();
            for (name, values) in &evaluated {
                let scalar = ScalarValue::try_from_array(values, row)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                resolved.properties.push((
                    name.clone(),
                    scalar_to_ir_literal(&scalar)
                        .map_err(|error| GfError::Execution(error.to_string()))?,
                ));
            }
            resolved.computed_properties.clear();
            resolved_rows.push(resolved);
        }
    }
    Ok(resolved_rows)
}

fn positional_eval_expr(expr: DfExpr, schema: &DFSchema) -> Result<(DfExpr, DFSchema), GfError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    let expr = expr
        .transform_up(|expr| {
            let DfExpr::Column(column) = &expr else {
                return Ok(Transformed::no(expr));
            };
            let index = if column.relation.is_none() {
                schema
                    .iter()
                    .enumerate()
                    .find_map(|(index, (qualifier, field))| {
                        (qualifier.is_none() && field.name() == &column.name).then_some(index)
                    })
                    .map_or_else(|| schema.index_of_column(column), Ok)?
            } else {
                schema.index_of_column(column)?
            };
            Ok(Transformed::yes(DfExpr::Column(Column::from_name(
                format!("__gf_eval_{index}"),
            ))))
        })
        .map_err(|error| GfError::Plan(error.to_string()))?
        .data;
    let fields = schema
        .as_arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            (
                None,
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_name(format!("__gf_eval_{index}")),
                ),
            )
        })
        .collect();
    let schema = DFSchema::new_with_metadata(fields, HashMap::new())
        .map_err(|error| GfError::Plan(error.to_string()))?;
    Ok((expr, schema))
}

struct MatchedMergeNode {
    uuid: [u8; 16],
    node_id: u64,
    type_id: u32,
    label_ids: Vec<u32>,
    properties: HashMap<String, graphforge_ir::IrLiteral>,
}

struct MatchedMergeEdge {
    uuid: [u8; 16],
    src_uuid: [u8; 16],
    dst_uuid: [u8; 16],
    rel_type: String,
    properties: HashMap<String, graphforge_ir::IrLiteral>,
}

fn find_matching_merge_nodes(
    env: &PhaseEnv<'_>,
    writer: &graphforge_storage::GraphWriter,
    spec: &ResolvedNodeSpec,
    deleted: &HashSet<[u8; 16]>,
) -> Result<Vec<MatchedMergeNode>, GfError> {
    let mut matches = writer
        .find_pending_nodes(&spec.label_ids, &spec.properties)
        .into_iter()
        .map(|found| MatchedMergeNode {
            uuid: found.0,
            node_id: found.1,
            type_id: found.2,
            label_ids: found.3,
            properties: found.4,
        })
        .collect::<Vec<_>>();
    let batches =
        graphforge_storage::read_nodes(env.dir).map_err(|e| GfError::Storage(e.to_string()))?;
    for batch in batches {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|a| {
                a.as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            })
            .ok_or_else(|| GfError::Storage("node topology missing node_uuid".into()))?;
        let node_ids = batch
            .column_by_name("node_id")
            .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| GfError::Storage("node topology missing node_id".into()))?;
        let primary = batch
            .column_by_name("type_id")
            .and_then(|a| a.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| GfError::Storage("node topology missing type_id".into()))?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
        for row in 0..batch.num_rows() {
            let row_labels = labels.value(row);
            let row_labels = row_labels
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| GfError::Storage("node type_ids are not UInt32".into()))?;
            if !spec
                .label_ids
                .iter()
                .all(|wanted| row_labels.values().iter().any(|actual| actual == wanted))
            {
                continue;
            }
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(uuids.value(row));
            if deleted.contains(&uuid) {
                continue;
            }
            let stem = if matches!(env.mode, OntologyMode::Exploratory) {
                "_untyped"
            } else {
                spec.label_names.first().map_or("_untyped", String::as_str)
            };
            let props = graphforge_storage::read_entity_properties(env.dir, stem, &uuid, false)?;
            if spec
                .properties
                .iter()
                .all(|(name, value)| props.get(name) == Some(value))
            {
                matches.push(MatchedMergeNode {
                    uuid,
                    node_id: node_ids.value(row),
                    type_id: primary.value(row),
                    label_ids: row_labels.values().to_vec(),
                    properties: props,
                });
            }
        }
    }
    Ok(matches)
}

#[allow(
    clippy::too_many_lines,
    reason = "per-row match expansion and create routing share one ordered frontier walk"
)]
fn run_relationship_merge_phase(
    env: &PhaseEnv<'_>,
    spec: &ResolvedEdgeSpec,
    on_create: &[MergeSetItem],
    on_match: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let row_specs = resolve_merge_edge_properties_by_row(env, spec, frontier)?;
    let rel_name = spec.rel_type_name.as_deref().ok_or_else(|| {
        GfError::Plan("relationship MERGE requires exactly one relationship type".into())
    })?;
    let src = WriteCol::resolve(&frontier.df_schema, spec.src, false, "")
        .ok_or_else(|| GfError::Plan("MERGE source node is not bound".into()))?;
    let dst = WriteCol::resolve(&frontier.df_schema, spec.dst, false, "")
        .ok_or_else(|| GfError::Plan("MERGE destination node is not bound".into()))?;
    let src_id_idx = frontier
        .df_schema
        .index_of_column_by_name(
            Some(&datafusion::common::TableReference::bare(format!(
                "var_{}",
                spec.src
            ))),
            "node_id",
        )
        .ok_or_else(|| GfError::Plan("MERGE source node has no node_id".into()))?;
    let dst_id_idx = frontier
        .df_schema
        .index_of_column_by_name(
            Some(&datafusion::common::TableReference::bare(format!(
                "var_{}",
                spec.dst
            ))),
            "node_id",
        )
        .ok_or_else(|| GfError::Plan("MERGE destination node has no node_id".into()))?;
    let edge_batches = graphforge_storage::read_edges(env.dir, rel_name, env.mode)
        .map_err(|e| GfError::Storage(e.to_string()))?;
    let mut edge_rows = Vec::with_capacity(frontier.num_rows());
    let mut created = Vec::with_capacity(frontier.num_rows());
    let mut input_rows = Vec::with_capacity(frontier.num_rows());

    let mut spec_row = 0usize;
    let mut input_row = 0u64;
    for batch in &frontier.batches {
        for row in 0..batch.num_rows() {
            let row_spec = &row_specs[spec_row];
            spec_row += 1;
            reject_null_merge_properties(&row_spec.properties)?;
            let src_uuid = fixed_binary_uuid(batch, src.uuid_idx, row)?;
            let dst_uuid = fixed_binary_uuid(batch, dst.uuid_idx, row)?;
            let src_bytes = to_bytes(&src_uuid);
            let dst_bytes = to_bytes(&dst_uuid);
            let matches = find_matching_merge_edges(
                env,
                &ctx.writer,
                row_spec,
                rel_name,
                &src_bytes,
                &dst_bytes,
                &edge_batches,
                &ctx.deleted,
            )?;
            if !matches.is_empty() {
                for matched in matches {
                    edge_rows.push(matched);
                    created.push(false);
                    input_rows.push(input_row);
                }
                input_row += 1;
                continue;
            }

            let src_id = merge_node_id_at(batch, src_id_idx, row, "source")?;
            let dst_id = merge_node_id_at(batch, dst_id_idx, row, "destination")?;
            ctx.writer.register_existing_node(src_uuid, src_id);
            ctx.writer.register_existing_node(dst_uuid, dst_id);
            let edge_value = graphforge_core::uuid::new_v7();
            ctx.writer
                .create_edge(edge_value, rel_name, &src_uuid, &dst_uuid)?;
            ctx.writer.set_edge_properties(
                &edge_value,
                Some(rel_name),
                row_spec.properties.iter().cloned().collect(),
            )?;
            edge_rows.push(MatchedMergeEdge {
                uuid: graphforge_core::uuid::to_bytes(&edge_value),
                src_uuid: src_bytes,
                dst_uuid: dst_bytes,
                rel_type: rel_name.to_owned(),
                properties: row_spec.properties.iter().cloned().collect(),
            });
            created.push(true);
            input_rows.push(input_row);
            input_row += 1;
            ctx.counters.edges_created += 1;
            ctx.counters.properties_set += row_spec.properties.len() as u64;
        }
    }
    frontier.take_rows(&input_rows)?;
    let property_names = edge_rows
        .iter()
        .flat_map(|row| row.properties.keys().cloned())
        .collect::<HashSet<_>>();
    frontier.rename_unqualified_collisions(&property_names, var_map)?;
    frontier.append_merged_edge_rows(spec.var, &edge_rows)?;
    var_map.insert(VarId(spec.var), format!("var_{}", spec.var));
    for (row, was_created) in edge_rows.iter().zip(&created) {
        let kind = if *was_created {
            crate::MutationKind::MergeCreate
        } else {
            crate::MutationKind::MergeMatchedNoop
        };
        ctx.record_mutation_input(kind, crate::MutationSubjectKind::Node, row.src_uuid);
        ctx.record_mutation_input(kind, crate::MutationSubjectKind::Node, row.dst_uuid);
        if *was_created {
            ctx.record_mutation_output(kind, crate::MutationSubjectKind::Edge, row.uuid);
        } else {
            ctx.record_mutation_input(kind, crate::MutationSubjectKind::Edge, row.uuid);
        }
    }
    let any_created = created.iter().any(|value| *value);
    let any_matched = created.iter().any(|value| !*value);
    match (any_created, any_matched) {
        (true, false) => run_merge_actions(env, on_create, frontier, var_map, ctx),
        (false, true) => run_merge_actions(env, on_match, frontier, var_map, ctx),
        (true, true) => {
            run_merge_actions_masked(env, on_create, frontier, var_map, ctx, &created)?;
            let matched = created.iter().map(|value| !value).collect::<Vec<_>>();
            run_merge_actions_masked(env, on_match, frontier, var_map, ctx, &matched)
        }
        (false, false) => Ok(()),
    }
}

fn resolve_merge_edge_properties_by_row(
    env: &PhaseEnv<'_>,
    spec: &ResolvedEdgeSpec,
    frontier: &Frontier,
) -> Result<Vec<ResolvedEdgeSpec>, GfError> {
    let mut physical = Vec::with_capacity(spec.computed_properties.len());
    for (name, expr) in &spec.computed_properties {
        let expr = bind_expr_params(expr.clone(), env.params)?;
        let (expr, eval_schema) = positional_eval_expr(expr, &frontier.df_schema)?;
        let expr = create_physical_expr(&expr, &eval_schema, &ExecutionProps::new())
            .map_err(|error| GfError::Plan(error.to_string()))?;
        physical.push((name, expr));
    }
    let mut resolved_rows = Vec::with_capacity(frontier.num_rows());
    for batch in &frontier.batches {
        let evaluated = physical
            .iter()
            .map(|(name, expr)| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
                    .map(|values| ((*name).clone(), values))
                    .map_err(|error| GfError::Execution(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let mut resolved = spec.clone();
            for (name, values) in &evaluated {
                let scalar = ScalarValue::try_from_array(values, row)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                resolved.properties.push((
                    name.clone(),
                    scalar_to_ir_literal(&scalar)
                        .map_err(|error| GfError::Execution(error.to_string()))?,
                ));
            }
            resolved.computed_properties.clear();
            resolved_rows.push(resolved);
        }
    }
    Ok(resolved_rows)
}

fn merge_node_id_at(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    endpoint: &str,
) -> Result<u64, GfError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .map(|ids| ids.value(row))
        .ok_or_else(|| GfError::Execution(format!("MERGE {endpoint} node_id is not UInt64")))
}

fn reject_null_merge_properties(
    properties: &[(String, graphforge_ir::IrLiteral)],
) -> Result<(), GfError> {
    if let Some((name, _)) = properties
        .iter()
        .find(|(_, value)| matches!(value, graphforge_ir::IrLiteral::Null))
    {
        return Err(GfError::Execution(format!(
            "MERGE property `{name}` cannot be null"
        )));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "edge matching requires storage, pattern, endpoints, batches, and statement deletes"
)]
fn find_matching_merge_edges(
    env: &PhaseEnv<'_>,
    writer: &graphforge_storage::GraphWriter,
    spec: &ResolvedEdgeSpec,
    rel_name: &str,
    wanted_src: &[u8; 16],
    wanted_dst: &[u8; 16],
    batches: &[RecordBatch],
    deleted: &HashSet<[u8; 16]>,
) -> Result<Vec<MatchedMergeEdge>, GfError> {
    let mut matches = Vec::new();
    if let Some((uuid, src_uuid, dst_uuid, properties)) = writer.find_pending_edge(
        rel_name,
        wanted_src,
        wanted_dst,
        matches!(spec.direction, Direction::Undirected),
        &spec.properties,
    ) {
        matches.push(MatchedMergeEdge {
            uuid,
            src_uuid,
            dst_uuid,
            rel_type: rel_name.to_owned(),
            properties,
        });
    }
    for batch in batches {
        let edge = uuid_column(batch, "edge_uuid")?;
        let src = uuid_column(batch, "src_uuid")?;
        let dst = uuid_column(batch, "dst_uuid")?;
        let names = batch
            .column_by_name("rel_type_name")
            .and_then(|a| a.as_any().downcast_ref::<arrow::array::StringArray>());
        for row in 0..batch.num_rows() {
            if names.is_some_and(|names| names.value(row) != rel_name) {
                continue;
            }
            let directed = src.value(row) == wanted_src && dst.value(row) == wanted_dst;
            let reverse = src.value(row) == wanted_dst && dst.value(row) == wanted_src;
            if !(directed || matches!(spec.direction, Direction::Undirected) && reverse) {
                continue;
            }
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(edge.value(row));
            if deleted.contains(&uuid) {
                continue;
            }
            let props = graphforge_storage::read_entity_properties(env.dir, rel_name, &uuid, true)?;
            if spec
                .properties
                .iter()
                .all(|(name, value)| props.get(name) == Some(value))
            {
                let mut src_uuid = [0u8; 16];
                src_uuid.copy_from_slice(src.value(row));
                let mut dst_uuid = [0u8; 16];
                dst_uuid.copy_from_slice(dst.value(row));
                matches.push(MatchedMergeEdge {
                    uuid,
                    src_uuid,
                    dst_uuid,
                    rel_type: rel_name.to_owned(),
                    properties: props,
                });
            }
        }
    }
    Ok(matches)
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a arrow::array::FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref())
        .ok_or_else(|| GfError::Storage(format!("edge topology missing {name}")))
}

fn run_merge_actions(
    env: &PhaseEnv<'_>,
    actions: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let mask = vec![true; frontier.num_rows()];
    run_merge_actions_masked(env, actions, frontier, var_map, ctx, &mask)
}

fn run_merge_actions_masked(
    env: &PhaseEnv<'_>,
    actions: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
    mask: &[bool],
) -> Result<(), GfError> {
    if mask.len() != frontier.num_rows() {
        return Err(GfError::Execution(
            "MERGE action mask does not match frontier rows".into(),
        ));
    }
    let props = actions
        .iter()
        .filter_map(|action| match action {
            MergeSetItem::Property(item) => Some(item.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let maps = actions
        .iter()
        .filter_map(|action| match action {
            MergeSetItem::Map(item) => Some(item.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for action in actions {
        let MergeSetItem::AddLabels { target, labels } = action else {
            continue;
        };
        let identity = WriteCol::resolve(&frontier.df_schema, target.0, false, "")
            .ok_or_else(|| GfError::Plan("MERGE label target is not a bound node".into()))?;
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", target.0));
        let type_ids_idx = frontier
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan("MERGE label target has no type_ids".into()))?;
        let label_ids = labels.iter().map(|label| label.0).collect::<Vec<_>>();
        let mut offset = 0usize;
        for batch in &frontier.batches {
            for row in 0..batch.num_rows() {
                let selected = mask[offset + row];
                if !selected {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                let existing = batch
                    .column(type_ids_idx)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not a list".into()))?
                    .value(row);
                let existing = existing
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not UInt32".into()))?;
                let missing = label_ids
                    .iter()
                    .copied()
                    .filter(|label| !existing.values().contains(label))
                    .collect::<Vec<_>>();
                let added = if ctx.writer.contains_pending_node(&uuid) {
                    ctx.writer.add_pending_node_labels(&uuid, &missing)
                } else {
                    let entry = ctx.label_additions.entry(uuid).or_default();
                    let before = entry.len();
                    entry.extend(missing.iter().copied());
                    (entry.len() - before) as u64
                };
                if added > 0 {
                    ctx.record_label_tokens(missing.iter().copied());
                }
            }
            offset += batch.num_rows();
        }
        frontier.add_node_labels(*target, &label_ids, mask)?;
    }
    run_set_phase_masked(env, &props, frontier, var_map, ctx, Some(mask), false)?;
    if !maps.is_empty() && mask.iter().any(|selected| !selected) {
        return Err(GfError::Plan(
            "row-conditional MERGE map actions are not implemented yet".into(),
        ));
    }
    run_set_map_phase_with_input(env, &maps, frontier, var_map, ctx, false)
}

/// Resolve each DELETE target's identity column against the frontier.
fn resolve_delete_cols(schema: &DFSchema, vars: &[VarId]) -> Result<Vec<DeleteCol>, GfError> {
    vars.iter()
        .map(|var| {
            let qual = datafusion::common::TableReference::bare(format!("var_{}", var.0));
            if let Some(uuid_idx) = schema.index_of_column_by_name(Some(&qual), "node_uuid") {
                Ok(DeleteCol {
                    uuid_idx,
                    is_edge: false,
                })
            } else if let Some(uuid_idx) = schema.index_of_column_by_name(Some(&qual), "edge_uuid")
            {
                Ok(DeleteCol {
                    uuid_idx,
                    is_edge: true,
                })
            } else {
                Err(GfError::Plan(format!(
                    "DELETE target var_{} has no node_uuid/edge_uuid column in the \
                     input — it must be bound by a preceding MATCH or CREATE",
                    var.0
                )))
            }
        })
        .collect()
}

fn collect_delete_expr_targets(
    env: &PhaseEnv<'_>,
    exprs: &[ExprId],
    frontier: &Frontier,
    var_map: &VarMap,
    nodes: &mut HashSet<[u8; 16]>,
    edges: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    for expr in exprs {
        let df_expr = env.lowerer.lower_value_expr_with_input(
            env.exprs,
            var_map,
            *expr,
            Arc::new(frontier.df_schema.clone()),
        )?;
        let physical = create_physical_expr(
            &bind_expr_params(df_expr, env.params)?,
            &frontier.df_schema,
            &ExecutionProps::new(),
        )
        .map_err(|error| GfError::Plan(error.to_string()))?;
        for batch in &frontier.batches {
            let values = physical
                .evaluate(batch)
                .and_then(|value| value.into_array(batch.num_rows()))
                .map_err(|error| GfError::Execution(error.to_string()))?;
            for row in 0..batch.num_rows() {
                let value = ScalarValue::try_from_array(&values, row)
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                collect_delete_scalar(&value, nodes, edges)?;
            }
        }
    }
    Ok(())
}

fn collect_delete_scalar(
    value: &ScalarValue,
    nodes: &mut HashSet<[u8; 16]>,
    edges: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    if value.is_null() {
        return Ok(());
    }
    if let Some(decoded) = graphforge_rel::expr::decode_het_scalar(value) {
        return collect_delete_scalar(&decoded, nodes, edges);
    }
    match value {
        ScalarValue::Struct(values) => {
            if let Some(uuid) = scalar_struct_uuid(values, "node_uuid")? {
                nodes.insert(uuid);
                return Ok(());
            }
            if let Some(uuid) = scalar_struct_uuid(values, "edge_uuid")? {
                edges.insert(uuid);
                return Ok(());
            }
            let mut found_path = false;
            for field in ["nodes", "relationships"] {
                if let Some(column) = values.column_by_name(field) {
                    found_path = true;
                    let nested = ScalarValue::try_from_array(column, 0)
                        .map_err(|error| GfError::Execution(error.to_string()))?;
                    collect_delete_scalar(&nested, nodes, edges)?;
                }
            }
            if found_path {
                Ok(())
            } else {
                Err(GfError::Execution(
                    "DELETE target must evaluate to a node, relationship, or path".into(),
                ))
            }
        }
        ScalarValue::List(values) => collect_delete_list(&values.value(0), nodes, edges),
        ScalarValue::LargeList(values) => collect_delete_list(&values.value(0), nodes, edges),
        _ => Err(GfError::Execution(
            "DELETE target must evaluate to a node, relationship, or path".into(),
        )),
    }
}

fn collect_delete_list(
    items: &ArrayRef,
    nodes: &mut HashSet<[u8; 16]>,
    edges: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    for row in 0..items.len() {
        let item = ScalarValue::try_from_array(items, row)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        collect_delete_scalar(&item, nodes, edges)?;
    }
    Ok(())
}

fn scalar_struct_uuid(values: &StructArray, field: &str) -> Result<Option<[u8; 16]>, GfError> {
    let Some(column) = values.column_by_name(field) else {
        return Ok(None);
    };
    if column.is_null(0) {
        return Ok(None);
    }
    let bytes = column
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .ok_or_else(|| GfError::Execution(format!("DELETE {field} is not a UUID")))?
        .value(0);
    if bytes.len() != 16 {
        return Err(GfError::Execution(format!(
            "DELETE {field} has invalid UUID width"
        )));
    }
    let mut uuid = [0; 16];
    uuid.copy_from_slice(bytes);
    Ok(Some(uuid))
}

/// DELETE phase: collect targets from the frontier, enforce the openCypher
/// incident-edge rule against committed **and** pending edges, cancel
/// pending-created targets in the buffer, and queue committed targets for the
/// commit-time rewrite.
fn run_delete_phase(
    env: &PhaseEnv<'_>,
    vars: &[VarId],
    exprs: &[ExprId],
    detach: bool,
    frontier: &Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let cols = resolve_delete_cols(&frontier.df_schema, vars)?;
    let mut node_targets: HashSet<[u8; 16]> = HashSet::new();
    let mut edge_targets: HashSet<[u8; 16]> = HashSet::new();
    for batch in &frontier.batches {
        collect_delete_targets(batch, &cols, &mut node_targets, &mut edge_targets)?;
    }
    collect_delete_expr_targets(
        env,
        exprs,
        frontier,
        var_map,
        &mut node_targets,
        &mut edge_targets,
    )?;
    // Deleting an already-deleted entity is a no-op.
    node_targets.retain(|u| !ctx.deleted.contains(u));
    edge_targets.retain(|u| !ctx.deleted.contains(u));

    // Incident edges: committed files for committed targets, plus the
    // statement's own pending buffer (an edge created earlier in this
    // statement counts, #792). A pending-created node cannot have committed
    // incident edges.
    let pending_nodes: HashSet<[u8; 16]> = node_targets
        .iter()
        .copied()
        .filter(|u| ctx.writer.contains_pending_node(u))
        .collect();
    let committed_nodes: HashSet<[u8; 16]> =
        node_targets.difference(&pending_nodes).copied().collect();
    let mut removed_labels = ctx.writer.pending_node_labels(&pending_nodes);
    let mut committed_labels = persisted_node_labels(env.dir, &committed_nodes)?;
    for uuid in &committed_nodes {
        let labels = committed_labels.entry(*uuid).or_default();
        if let Some(additions) = ctx.label_additions.get(uuid) {
            labels.extend(additions);
        }
        if let Some(removals) = ctx.label_removals.get(uuid) {
            labels.retain(|label| !removals.contains(label));
        }
    }
    removed_labels.extend(committed_labels.into_values().flatten());
    if !removed_labels.is_empty() {
        let surviving_labels = surviving_node_labels(env.dir, &node_targets, ctx)?;
        removed_labels.retain(|label| !surviving_labels.contains(label));
    }
    ctx.record_removed_label_tokens(removed_labels);
    let mut incident: HashSet<[u8; 16]> =
        graphforge_storage::incident_edge_uuids(env.dir, &committed_nodes)?
            .into_iter()
            .collect();
    incident.extend(ctx.writer.pending_incident_edge_uuids(&node_targets));

    let survivors: Vec<[u8; 16]> = incident
        .into_iter()
        .filter(|e| !edge_targets.contains(e) && !ctx.deleted.contains(e))
        .collect();
    if detach {
        edge_targets.extend(survivors);
    } else if !survivors.is_empty() {
        return Err(GfError::Execution(
            "Cannot delete node, because it still has relationships. To delete \
             this node, you must first delete its relationships, or use DETACH DELETE."
                .into(),
        ));
    }
    let mutation_kind = if detach {
        crate::MutationKind::DetachDelete
    } else {
        crate::MutationKind::Delete
    };
    for uuid in &node_targets {
        ctx.record_mutation_input(mutation_kind, crate::MutationSubjectKind::Node, *uuid);
    }
    for uuid in &edge_targets {
        ctx.record_mutation_input(mutation_kind, crate::MutationSubjectKind::Edge, *uuid);
    }

    // Edges: cancel pending-created ones in the buffer (they never hit disk),
    // queue committed ones for the commit-time rewrite. Both count.
    let pending_edges: HashSet<[u8; 16]> = edge_targets
        .iter()
        .copied()
        .filter(|u| ctx.writer.contains_pending_edge(u))
        .collect();
    let committed_edges: HashSet<[u8; 16]> =
        edge_targets.difference(&pending_edges).copied().collect();
    ctx.counters.properties_removed +=
        graphforge_storage::count_entity_properties(env.dir, &committed_edges, true)?;
    ctx.counters.edges_deleted += edge_targets.len() as u64;
    ctx.writer.cancel_edges(&pending_edges);
    ctx.pending_edge_deletes.extend(&committed_edges);
    ctx.deleted.extend(edge_targets.iter().copied());

    // Nodes, likewise.
    ctx.counters.properties_removed +=
        graphforge_storage::count_entity_properties(env.dir, &committed_nodes, false)?;
    ctx.counters.nodes_deleted += node_targets.len() as u64;
    ctx.writer.cancel_nodes(&pending_nodes);
    ctx.pending_node_deletes.extend(committed_nodes);
    ctx.deleted.extend(node_targets);
    Ok(())
}

fn persisted_node_labels(
    dir: &Path,
    targets: &HashSet<[u8; 16]>,
) -> Result<HashMap<[u8; 16], HashSet<u32>>, GfError> {
    if targets.is_empty() {
        return Ok(HashMap::new());
    }
    let mut found = HashMap::new();
    for batch in
        graphforge_storage::read_nodes(dir).map_err(|error| GfError::Storage(error.to_string()))?
    {
        collect_node_label_batch(&batch, Some(targets), &mut found)?;
    }
    Ok(found)
}

fn surviving_node_labels(
    dir: &Path,
    deleting: &HashSet<[u8; 16]>,
    ctx: &StatementWriteContext,
) -> Result<HashSet<u32>, GfError> {
    let mut nodes = HashMap::new();
    for batch in
        graphforge_storage::read_nodes(dir).map_err(|error| GfError::Storage(error.to_string()))?
    {
        collect_node_label_batch(&batch, None, &mut nodes)?;
    }
    collect_node_label_batch(&ctx.writer.pending_nodes_batch()?, None, &mut nodes)?;
    for uuid in deleting
        .iter()
        .chain(&ctx.pending_node_deletes)
        .chain(&ctx.deleted)
    {
        nodes.remove(uuid);
    }
    for (uuid, additions) in &ctx.label_additions {
        if let Some(labels) = nodes.get_mut(uuid) {
            labels.extend(additions);
        }
    }
    for (uuid, removals) in &ctx.label_removals {
        if let Some(labels) = nodes.get_mut(uuid) {
            labels.retain(|label| !removals.contains(label));
        }
    }
    Ok(nodes.into_values().flatten().collect())
}

fn collect_node_label_batch(
    batch: &RecordBatch,
    targets: Option<&HashSet<[u8; 16]>>,
    found: &mut HashMap<[u8; 16], HashSet<u32>>,
) -> Result<(), GfError> {
    let uuids = batch
        .column_by_name("node_uuid")
        .and_then(|array| {
            array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        })
        .ok_or_else(|| GfError::Storage("node topology missing node_uuid".into()))?;
    let labels = batch
        .column_by_name("type_ids")
        .and_then(|array| array.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
    for row in 0..batch.num_rows() {
        if uuids.is_null(row) || uuids.value_length() != 16 {
            continue;
        }
        let mut uuid = [0; 16];
        uuid.copy_from_slice(uuids.value(row));
        if targets.is_some_and(|targets| !targets.contains(&uuid)) {
            continue;
        }
        let values = labels.value(row);
        let values = values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| GfError::Storage("node labels are not UInt32".into()))?;
        found
            .entry(uuid)
            .or_default()
            .extend(values.values().iter().copied());
    }
    Ok(())
}

/// Mirror of the lowerer's `resolve_write_kind` against the frontier: node
/// (`false`) when `var_<n>.node_uuid` exists, edge (`true`) when
/// `var_<n>.edge_uuid` + `rel_type_name` exist.
fn resolve_kind(schema: &DFSchema, var: VarId, clause: &str) -> Result<bool, GfError> {
    let qual = datafusion::common::TableReference::bare(format!("var_{}", var.0));
    let is_node = schema
        .index_of_column_by_name(Some(&qual), "node_uuid")
        .is_some();
    let is_edge = schema
        .index_of_column_by_name(Some(&qual), "edge_uuid")
        .is_some();
    match (is_node, is_edge) {
        (true, _) => Ok(false),
        (false, true) => {
            if schema
                .index_of_column_by_name(Some(&qual), "rel_type_name")
                .is_some()
            {
                Ok(true)
            } else {
                Err(GfError::Plan(format!(
                    "{clause} on an edge requires a known relation type \
                     (e.g. `-[r:KNOWS]->`); an untyped edge write is not yet \
                     supported (follow-up to #791)"
                )))
            }
        }
        (false, false) => Err(GfError::Plan(format!(
            "{clause} target var_{} has no node_uuid/edge_uuid column in the \
             input — it must be bound by a preceding MATCH",
            var.0
        ))),
    }
}

/// SET phase: evaluate each item's value per frontier row; route the write to
/// the pending buffer (created entities), reject deleted targets, accumulate
/// the rest for the commit-time rewrite.
fn run_set_phase(
    env: &PhaseEnv<'_>,
    items: &[SetPropItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    run_set_phase_masked(env, items, frontier, var_map, ctx, None, true)
}

#[allow(
    clippy::too_many_lines,
    reason = "one row loop keeps masked evaluation, persistence routing, counters, and overlays aligned"
)]
fn run_set_phase_masked(
    env: &PhaseEnv<'_>,
    items: &[SetPropItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
    mask: Option<&[bool]>,
    use_input_types: bool,
) -> Result<(), GfError> {
    if mask.is_some_and(|mask| mask.len() != frontier.num_rows()) {
        return Err(GfError::Execution(
            "SET row mask does not match frontier rows".into(),
        ));
    }
    for item in items {
        let is_edge = resolve_kind(&frontier.df_schema, item.target, "SET")?;
        let col = WriteCol::resolve(&frontier.df_schema, item.target.0, is_edge, &item.prop_name)
            .ok_or_else(|| {
            GfError::Plan(format!(
                "SET target var_{} has no identity column in the input",
                item.target.0
            ))
        })?;
        let df_expr = if use_input_types {
            env.lowerer.lower_value_expr_with_input(
                env.exprs,
                var_map,
                item.value,
                Arc::new(frontier.df_schema.clone()),
            )?
        } else {
            env.lowerer
                .lower_value_expr(env.exprs, var_map, item.value)?
        };
        let df_expr = bind_expr_params(df_expr, env.params)?;
        let (df_expr, eval_schema) = positional_eval_expr(df_expr, &frontier.df_schema)?;
        let phys = create_physical_expr(&df_expr, &eval_schema, &ExecutionProps::new())
            .map_err(|e| GfError::Plan(e.to_string()))?;

        let mut overlay = Vec::with_capacity(frontier.batches.len());
        let mut offset = 0usize;
        for batch in &frontier.batches {
            let n = batch.num_rows();
            let values = phys
                .evaluate(batch)
                .and_then(|cv| cv.into_array(n))
                .map_err(|e| GfError::Execution(e.to_string()))?;
            let selected = mask.map(|mask| &mask[offset..offset + n]);
            let overlay_values = if let Some(selected) = selected {
                let selection = BooleanArray::from(selected.to_vec());
                let previous = frontier
                    .df_schema
                    .index_of_column_by_name(
                        Some(&datafusion::common::TableReference::bare(format!(
                            "var_{}",
                            item.target.0
                        ))),
                        &item.prop_name,
                    )
                    .map_or_else(
                        || {
                            ScalarValue::try_new_null(values.data_type())
                                .and_then(|value| value.to_array_of_size(n))
                        },
                        |index| Ok(Arc::clone(batch.column(index))),
                    )
                    .map_err(|error| GfError::Execution(error.to_string()))?;
                arrow::compute::kernels::zip::zip(&selection, &values, &previous)
                    .map_err(|error| GfError::Execution(error.to_string()))?
            } else {
                Arc::clone(&values)
            };
            overlay.push(overlay_values);
            let id_col = batch.column(col.uuid_idx);
            for row in 0..n {
                if selected.is_some_and(|selected| !selected[row]) {
                    continue;
                }
                if id_col.is_null(row) {
                    continue; // SET on a NULL (unmatched OPTIONAL row) is a no-op
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot SET a property on an entity deleted in this statement".into(),
                    ));
                }
                let scalar = ScalarValue::try_from_array(&values, row)
                    .map_err(|e| GfError::Execution(e.to_string()))?;
                let stem = col.stem_for_row(batch, row, env.mode, &env.type_map)?;
                if scalar.is_null() {
                    let present = property_is_present(
                        &frontier.df_schema,
                        batch,
                        item.target,
                        &item.prop_name,
                        row,
                    );
                    remove_map_complement(
                        ctx,
                        is_edge,
                        &uuid,
                        &stem,
                        &HashSet::from([item.prop_name.clone()]),
                    );
                    if present {
                        ctx.counters.properties_removed += 1;
                        ctx.record_mutation_output(
                            crate::MutationKind::RemoveProperty,
                            if is_edge {
                                crate::MutationSubjectKind::Edge
                            } else {
                                crate::MutationSubjectKind::Node
                            },
                            uuid,
                        );
                    }
                    continue;
                }
                let lit =
                    scalar_to_ir_literal(&scalar).map_err(|e| GfError::Execution(e.to_string()))?;
                let pending = if is_edge {
                    ctx.writer.contains_pending_edge(&uuid)
                } else {
                    ctx.writer.contains_pending_node(&uuid)
                };
                if is_edge && pending {
                    ctx.writer.merge_pending_edge_props(
                        &uuid,
                        Some(&stem),
                        HashMap::from([(item.prop_name.clone(), lit)]),
                    );
                } else if !is_edge && pending {
                    ctx.writer.merge_pending_node_props(
                        &uuid,
                        Some(&stem),
                        HashMap::from([(item.prop_name.clone(), lit)]),
                    );
                } else {
                    ctx.remove_acc
                        .forget(is_edge, &stem, &uuid, &item.prop_name);
                    ctx.set_acc
                        .record(is_edge, stem, uuid, item.prop_name.clone(), lit);
                }
                let replaced = property_is_present(
                    &frontier.df_schema,
                    batch,
                    item.target,
                    &item.prop_name,
                    row,
                );
                let recorded = if pending && replaced {
                    false
                } else {
                    ctx.record_property_set(is_edge, uuid, &item.prop_name)
                };
                if recorded && replaced {
                    ctx.counters.properties_removed += 1;
                }
                ctx.record_mutation_output(
                    crate::MutationKind::SetProperty,
                    if is_edge {
                        crate::MutationSubjectKind::Edge
                    } else {
                        crate::MutationSubjectKind::Node
                    },
                    uuid,
                );
            }
            offset += n;
        }
        frontier.overlay_property(item.target, &item.prop_name, overlay)?;
        if !is_edge {
            env.lowerer
                .register_node_property_shape(item.target, &item.prop_name);
        }
    }
    Ok(())
}

fn run_set_map_phase(
    env: &PhaseEnv<'_>,
    items: &[SetMapItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    run_set_map_phase_with_input(env, items, frontier, var_map, ctx, true)
}

#[allow(
    clippy::too_many_lines,
    reason = "plain and tagged map writes share replacement accounting and frontier overlays"
)]
fn run_set_map_phase_with_input(
    env: &PhaseEnv<'_>,
    items: &[SetMapItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
    use_input_types: bool,
) -> Result<(), GfError> {
    for item in items {
        let is_edge = resolve_kind(&frontier.df_schema, item.target, "SET")?;
        let identity = WriteCol::resolve(&frontier.df_schema, item.target.0, is_edge, "")
            .ok_or_else(|| {
                GfError::Plan(format!(
                    "SET target var_{} has no identity column in the input",
                    item.target.0
                ))
            })?;
        let existing_names = entity_property_names(&frontier.df_schema, item.target);
        let df_expr = if use_input_types {
            env.lowerer.lower_value_expr_with_input(
                env.exprs,
                var_map,
                item.map,
                Arc::new(frontier.df_schema.clone()),
            )?
        } else {
            env.lowerer.lower_value_expr(env.exprs, var_map, item.map)?
        };
        let df_expr = bind_expr_params(df_expr, env.params)?;
        let (df_expr, eval_schema) = positional_eval_expr(df_expr, &frontier.df_schema)?;
        let phys = create_physical_expr(&df_expr, &eval_schema, &ExecutionProps::new())
            .map_err(|e| GfError::Plan(e.to_string()))?;
        let mut overlays: HashMap<String, Vec<ArrayRef>> = HashMap::new();

        for batch in &frontier.batches {
            let values = phys
                .evaluate(batch)
                .and_then(|cv| cv.into_array(batch.num_rows()))
                .map_err(|e| GfError::Execution(e.to_string()))?;
            let maps = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    GfError::Execution("SET map expression must evaluate to a map".into())
                })?;
            if maps.column_by_name("__het_tag").is_some() {
                for row in 0..batch.num_rows() {
                    if maps.is_null(row) {
                        continue;
                    }
                    let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                    let stem = identity.stem_for_row(batch, row, env.mode, &env.type_map)?;
                    let updates = decode_tagged_map_updates(maps, row)?;
                    let mut present: HashSet<_> = existing_names
                        .iter()
                        .filter(|name| {
                            property_is_present(&frontier.df_schema, batch, item.target, name, row)
                        })
                        .cloned()
                        .collect();
                    if item.replace
                        && !ctx.writer.contains_pending_node(&uuid)
                        && !ctx.writer.contains_pending_edge(&uuid)
                    {
                        present.extend(graphforge_storage::read_entity_property_keys(
                            env.dir, &stem, &uuid, is_edge,
                        )?);
                    }
                    let removals = if item.replace {
                        present
                            .difference(&updates.keys().cloned().collect())
                            .cloned()
                            .collect::<HashSet<_>>()
                    } else {
                        HashSet::new()
                    };
                    let replaced = if item.replace {
                        present
                            .iter()
                            .filter(|name| updates.contains_key(*name))
                            .count()
                    } else {
                        0
                    };
                    remove_map_complement(ctx, is_edge, &uuid, &stem, &removals);
                    ctx.counters.properties_removed += (removals.len() + replaced) as u64;
                    for name in updates.keys() {
                        let _ = ctx.record_property_set(is_edge, uuid, name);
                    }
                    apply_map_updates(ctx, is_edge, &uuid, &stem, updates);
                }
                continue;
            }
            for (field, column) in maps.fields().iter().zip(maps.columns()) {
                if !is_edge {
                    env.lowerer
                        .register_node_property_shape(item.target, field.name());
                }
                overlays
                    .entry(field.name().clone())
                    .or_default()
                    .push(Arc::clone(column));
            }

            let id_col = batch.column(identity.uuid_idx);
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) || maps.is_null(row) {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot SET properties on an entity deleted in this statement".into(),
                    ));
                }
                let stem = identity.stem_for_row(batch, row, env.mode, &env.type_map)?;
                let mut updates = HashMap::with_capacity(maps.num_columns());
                let mut null_keys = HashSet::new();
                for (field, column) in maps.fields().iter().zip(maps.columns()) {
                    let scalar = ScalarValue::try_from_array(column, row)
                        .map_err(|e| GfError::Execution(e.to_string()))?;
                    if scalar.is_null() {
                        null_keys.insert(field.name().clone());
                    } else {
                        let value = scalar_to_ir_literal(&scalar)
                            .map_err(|e| GfError::Execution(e.to_string()))?;
                        updates.insert(field.name().clone(), value);
                    }
                }
                let mut present: HashSet<_> = existing_names
                    .iter()
                    .filter(|name| {
                        property_is_present(&frontier.df_schema, batch, item.target, name, row)
                    })
                    .cloned()
                    .collect();
                if item.replace
                    && !ctx.writer.contains_pending_node(&uuid)
                    && !ctx.writer.contains_pending_edge(&uuid)
                {
                    present.extend(graphforge_storage::read_entity_property_keys(
                        env.dir, &stem, &uuid, is_edge,
                    )?);
                }
                let (removals, replaced) =
                    map_removals(item.replace, &present, &updates, &null_keys);
                if !removals.is_empty() {
                    ctx.record_mutation_output(
                        crate::MutationKind::RemoveProperty,
                        if is_edge {
                            crate::MutationSubjectKind::Edge
                        } else {
                            crate::MutationSubjectKind::Node
                        },
                        uuid,
                    );
                }
                if !updates.is_empty() {
                    ctx.record_mutation_output(
                        crate::MutationKind::SetProperty,
                        if is_edge {
                            crate::MutationSubjectKind::Edge
                        } else {
                            crate::MutationSubjectKind::Node
                        },
                        uuid,
                    );
                }
                remove_map_complement(ctx, is_edge, &uuid, &stem, &removals);
                ctx.counters.properties_removed += replaced as u64;
                ctx.counters.properties_set += updates.len() as u64;
                apply_map_updates(ctx, is_edge, &uuid, &stem, updates);
            }
        }
        overlay_map_result(frontier, item, existing_names, overlays)?;
    }
    Ok(())
}

fn decode_tagged_map_updates(
    maps: &StructArray,
    row: usize,
) -> Result<HashMap<String, graphforge_ir::IrLiteral>, GfError> {
    let tagged = ScalarValue::try_from_array(maps, row)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    let decoded = graphforge_rel::expr::decode_het_scalar(&tagged)
        .ok_or_else(|| GfError::Execution("SET source is not a map".into()))?;
    let ScalarValue::Struct(values) = decoded else {
        return Err(GfError::Execution("SET source is not a map".into()));
    };
    let mut updates = HashMap::new();
    for (field, column) in values.fields().iter().zip(values.columns()) {
        let tagged_value = ScalarValue::try_from_array(column, 0)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        let value = graphforge_rel::expr::decode_het_scalar(&tagged_value).unwrap_or(tagged_value);
        if !value.is_null() {
            updates.insert(
                field.name().clone(),
                scalar_to_ir_literal(&value)
                    .map_err(|error| GfError::Execution(error.to_string()))?,
            );
        }
    }
    Ok(updates)
}

fn map_removals(
    replace: bool,
    present: &HashSet<String>,
    updates: &HashMap<String, graphforge_ir::IrLiteral>,
    null_keys: &HashSet<String>,
) -> (HashSet<String>, usize) {
    if replace {
        let removals = present
            .iter()
            .filter(|name| !updates.contains_key(*name))
            .cloned()
            .collect();
        (removals, present.len())
    } else {
        let removals = present.intersection(null_keys).cloned().collect();
        let replaced = present
            .iter()
            .filter(|name| updates.contains_key(*name) || null_keys.contains(*name))
            .count();
        (removals, replaced)
    }
}

fn overlay_map_result(
    frontier: &mut Frontier,
    item: &SetMapItem,
    existing_names: HashSet<String>,
    overlays: HashMap<String, Vec<ArrayRef>>,
) -> Result<(), GfError> {
    if item.replace {
        for name in existing_names {
            if !overlays.contains_key(&name) {
                let values = frontier
                    .batches
                    .iter()
                    .map(|batch| arrow::array::new_null_array(&DataType::Null, batch.num_rows()))
                    .collect();
                frontier.overlay_property(item.target, &name, values)?;
            }
        }
    }
    for (name, values) in overlays {
        frontier.overlay_property(item.target, &name, values)?;
    }
    Ok(())
}

fn entity_property_names(schema: &DFSchema, var: VarId) -> HashSet<String> {
    let qualifier = format!("var_{}", var.0);
    schema
        .iter()
        .filter(|(q, field)| {
            q.is_some_and(|q| q.to_string() == qualifier)
                && !matches!(
                    field.name().as_str(),
                    "node_uuid"
                        | "node_id"
                        | "type_id"
                        | "type_ids"
                        | "created_at"
                        | "updated_at"
                        | "edge_uuid"
                        | "src_uuid"
                        | "dst_uuid"
                        | "edge_id"
                        | "src_id"
                        | "dst_id"
                        | "rel_type_name"
                )
        })
        .map(|(_, field)| field.name().clone())
        .collect()
}

fn property_is_present(
    schema: &DFSchema,
    batch: &RecordBatch,
    var: VarId,
    name: &str,
    row: usize,
) -> bool {
    let qualifier = datafusion::common::TableReference::bare(format!("var_{}", var.0));
    schema
        .index_of_column_by_name(Some(&qualifier), name)
        .is_some_and(|index| !batch.column(index).is_null(row))
}

fn apply_map_updates(
    ctx: &mut StatementWriteContext,
    is_edge: bool,
    uuid: &[u8; 16],
    stem: &str,
    updates: HashMap<String, graphforge_ir::IrLiteral>,
) {
    if is_edge && ctx.writer.contains_pending_edge(uuid) {
        ctx.writer
            .merge_pending_edge_props(uuid, Some(stem), updates);
    } else if !is_edge && ctx.writer.contains_pending_node(uuid) {
        ctx.writer
            .merge_pending_node_props(uuid, Some(stem), updates);
    } else {
        for (name, value) in updates {
            ctx.remove_acc.forget(is_edge, stem, uuid, &name);
            ctx.set_acc
                .record(is_edge, stem.to_owned(), *uuid, name, value);
        }
    }
}

fn remove_map_complement(
    ctx: &mut StatementWriteContext,
    is_edge: bool,
    uuid: &[u8; 16],
    stem: &str,
    keys: &HashSet<String>,
) {
    if is_edge && ctx.writer.contains_pending_edge(uuid) {
        ctx.writer.remove_pending_edge_props(uuid, keys);
    } else if !is_edge && ctx.writer.contains_pending_node(uuid) {
        ctx.writer.remove_pending_node_props(uuid, keys);
    } else {
        for name in keys {
            ctx.set_acc.forget(is_edge, stem, uuid, name);
            ctx.remove_acc
                .record(is_edge, stem.to_owned(), *uuid, name.clone());
        }
    }
}

/// REMOVE phase: the value-less dual of [`run_set_phase`].
fn run_remove_phase(
    env: &PhaseEnv<'_>,
    items: &[graphforge_ir::RemovePropItem],
    frontier: &mut Frontier,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    for item in items {
        let is_edge = resolve_kind(&frontier.df_schema, item.target, "REMOVE")?;
        let col = WriteCol::resolve(&frontier.df_schema, item.target.0, is_edge, &item.prop_name)
            .ok_or_else(|| {
            GfError::Plan(format!(
                "REMOVE target var_{} has no identity column in the input",
                item.target.0
            ))
        })?;
        let existing = frontier.df_schema.index_of_column_by_name(
            Some(&datafusion::common::TableReference::bare(format!(
                "var_{}",
                item.target.0
            ))),
            &item.prop_name,
        );
        let overlay_type = existing.map_or(DataType::Null, |index| {
            frontier.batches[0].column(index).data_type().clone()
        });
        let mut overlay = Vec::with_capacity(frontier.batches.len());
        for batch in &frontier.batches {
            let id_col = batch.column(col.uuid_idx);
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot REMOVE a property from an entity deleted in this statement".into(),
                    ));
                }
                if !property_is_present(
                    &frontier.df_schema,
                    batch,
                    item.target,
                    &item.prop_name,
                    row,
                ) {
                    continue;
                }
                let keys = HashSet::from([item.prop_name.clone()]);
                if is_edge && ctx.writer.contains_pending_edge(&uuid) {
                    ctx.writer.remove_pending_edge_props(&uuid, &keys);
                } else if !is_edge && ctx.writer.contains_pending_node(&uuid) {
                    ctx.writer.remove_pending_node_props(&uuid, &keys);
                } else {
                    let stem = col.stem_for_row(batch, row, env.mode, &env.type_map)?;
                    ctx.set_acc.forget(is_edge, &stem, &uuid, &item.prop_name);
                    ctx.remove_acc
                        .record(is_edge, stem, uuid, item.prop_name.clone());
                }
                ctx.counters.properties_removed += 1;
                ctx.record_mutation_output(
                    crate::MutationKind::RemoveProperty,
                    if is_edge {
                        crate::MutationSubjectKind::Edge
                    } else {
                        crate::MutationSubjectKind::Node
                    },
                    uuid,
                );
            }
            overlay.push(
                ScalarValue::try_from(&overlay_type)
                    .unwrap_or(ScalarValue::Null)
                    .to_array_of_size(batch.num_rows())
                    .map_err(|e| GfError::Execution(e.to_string()))?,
            );
        }
        frontier.overlay_property(item.target, &item.prop_name, overlay)?;
    }
    Ok(())
}

fn run_label_phase(
    items: &[graphforge_ir::LabelItem],
    add: bool,
    frontier: &mut Frontier,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    for item in items {
        let identity = WriteCol::resolve(&frontier.df_schema, item.target.0, false, "")
            .ok_or_else(|| GfError::Plan("label mutation target is not a bound node".into()))?;
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", item.target.0));
        let type_ids_idx = frontier
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan("label mutation target has no type_ids".into()))?;
        let requested = item.labels.iter().map(|label| label.0).collect::<Vec<_>>();
        let mut seen = HashSet::new();
        for batch in &frontier.batches {
            let id_col = batch.column(identity.uuid_idx);
            let labels = batch
                .column(type_ids_idx)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Execution("node type_ids are not a list".into()))?;
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                if !seen.insert(uuid) {
                    continue;
                }
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot mutate labels on an entity deleted in this statement".into(),
                    ));
                }
                let values = labels.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not UInt32".into()))?;
                let changed = requested
                    .iter()
                    .copied()
                    .filter(|label| values.values().contains(label) != add)
                    .collect::<Vec<_>>();
                if changed.is_empty() {
                    continue;
                }
                let count = if ctx.writer.contains_pending_node(&uuid) {
                    if add {
                        ctx.writer.add_pending_node_labels(&uuid, &changed)
                    } else {
                        ctx.writer.remove_pending_node_labels(&uuid, &changed)
                    }
                } else if add {
                    if let Some(removals) = ctx.label_removals.get_mut(&uuid) {
                        for label in &changed {
                            removals.remove(label);
                        }
                    }
                    let additions = ctx.label_additions.entry(uuid).or_default();
                    let before = additions.len();
                    additions.extend(changed.iter().copied());
                    (additions.len() - before) as u64
                } else {
                    if let Some(additions) = ctx.label_additions.get_mut(&uuid) {
                        for label in &changed {
                            additions.remove(label);
                        }
                    }
                    let removals = ctx.label_removals.entry(uuid).or_default();
                    let before = removals.len();
                    removals.extend(changed.iter().copied());
                    (removals.len() - before) as u64
                };
                if count > 0 {
                    if add {
                        ctx.record_label_tokens(changed.iter().copied());
                    } else {
                        ctx.record_removed_label_tokens(changed.iter().copied());
                    }
                    ctx.record_mutation_output(
                        if add {
                            crate::MutationKind::AddLabel
                        } else {
                            crate::MutationKind::RemoveLabel
                        },
                        crate::MutationSubjectKind::Node,
                        uuid,
                    );
                }
            }
        }
        if add {
            let mask = vec![true; frontier.num_rows()];
            frontier.add_node_labels(item.target, &requested, &mask)?;
        } else {
            frontier.remove_node_labels(item.target, &requested)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Commit and summary
// ---------------------------------------------------------------------------

/// Stage every buffered effect into one batch and commit it: SET/REMOVE
/// rewrites, then edge deletes, then node deletes (`topology/nodes.parquet`
/// staged last, #790), then the writer's appends — which read **through** the
/// batch and restage in place, so each file commits exactly once with the
/// statement's net content.
///
/// A statement whose net batch stages topology files bumps the project
/// `topology_generation` counter exactly once, before the commit (#759);
/// SET/REMOVE-only statements do not bump.
pub(crate) fn commit_statement(ctx: &mut StatementWriteContext, dir: &Path) -> Result<(), GfError> {
    // Writes to entities deleted later in the statement are unobservable —
    // they must not resurrect rows in the rewrite.
    ctx.set_acc.scrub(&ctx.deleted);
    ctx.remove_acc.scrub(&ctx.deleted);

    let mut staged = graphforge_storage::RewriteBatch::new();
    ctx.set_acc.stage_into(&mut staged, dir)?;
    ctx.remove_acc.stage_into(&mut staged, dir)?;
    graphforge_storage::stage_mutate_node_labels(
        &mut staged,
        dir,
        &ctx.label_additions,
        &ctx.label_removals,
    )?;
    graphforge_storage::stage_delete_edges(&mut staged, dir, &ctx.pending_edge_deletes)?;
    graphforge_storage::stage_delete_nodes(&mut staged, dir, &ctx.pending_node_deletes)?;
    ctx.writer.flush_into(&mut staged)?;

    // Adjacency delta segment (#765): a statement is pure-append iff it stages
    // no deletes (SET/REMOVE never touch topology). Pure-append → record the
    // created edges so the index serves them without a rebuild; otherwise the
    // statement breaks the chain at this generation (a DELETE invalidates the
    // incremental path), so write no segment and clear any stale file there.
    let pure_append = ctx.pending_node_deletes.is_empty() && ctx.pending_edge_deletes.is_empty();
    let pending = ctx.writer.take_pending_delta();
    if let Some(generation) = graphforge_storage::commit_topology_aware(staged, dir)? {
        if pure_append {
            ctx.writer.write_segment_best_effort(generation, &pending);
        } else {
            graphforge_storage::adjacency_delta::discard_segment(dir, generation);
        }
    }
    Ok(())
}

/// The unified write-statement summary schema: six openCypher write counters.
pub(crate) fn statement_summary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("nodes_created", DataType::UInt64, false),
        Field::new("edges_created", DataType::UInt64, false),
        Field::new("nodes_deleted", DataType::UInt64, false),
        Field::new("edges_deleted", DataType::UInt64, false),
        Field::new("properties_set", DataType::UInt64, false),
        Field::new("properties_removed", DataType::UInt64, false),
    ]))
}

/// The one-row counter summary for a write statement.
pub(crate) fn statement_summary_batch(c: &WriteCounters) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        statement_summary_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![c.nodes_created])),
            Arc::new(UInt64Array::from(vec![c.edges_created])),
            Arc::new(UInt64Array::from(vec![c.nodes_deleted])),
            Arc::new(UInt64Array::from(vec![c.edges_deleted])),
            Arc::new(UInt64Array::from(vec![c.properties_set])),
            Arc::new(UInt64Array::from(vec![c.properties_removed])),
        ],
    )
    .map_err(|e| GfError::Execution(e.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryBuilder, Int64Array, StringArray, UInt32Array};
    use datafusion::common::TableReference;
    use graphforge_ir::{
        CreateEdgeSpec, CreateNodeSpec, CreatePattern, IrLiteral, PropId, RemovePropItem,
        SetPropItem, VarId,
    };

    use super::*;

    #[test]
    fn recreating_a_removed_label_token_cancels_its_removal() {
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
        ctx.known_labels.insert(7);

        ctx.record_removed_label_tokens([7]);
        ctx.record_label_tokens([7]);

        assert_eq!(ctx.counters.labels_removed, 0);
        assert_eq!(ctx.counters.labels_added, 0);
    }

    fn create_op() -> GraphOp {
        GraphOp::Create {
            pattern: CreatePattern::default(),
        }
    }

    fn merge_op() -> GraphOp {
        GraphOp::Merge {
            pattern: CreatePattern::default(),
            on_create: vec![],
            on_match: vec![],
        }
    }

    fn delete_op() -> GraphOp {
        GraphOp::Delete {
            vars: vec![VarId(0)],
            exprs: vec![],
            detach: false,
        }
    }

    fn scan_op() -> GraphOp {
        GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        }
    }

    fn project_op() -> GraphOp {
        GraphOp::Project {
            items: vec![],
            distinct: false,
        }
    }

    #[test]
    fn split_accepts_prefix_then_writes_in_order() {
        let ops = vec![scan_op(), create_op(), delete_op()];
        let split = split_write_plan(&ops).unwrap();
        assert_eq!(split.prefix_len, 1);
        assert_eq!(split.write_ops, vec![1, 2]);
        assert_eq!(split.read_suffix_start, None);
    }

    #[test]
    fn split_accepts_empty_prefix() {
        // Standalone CREATE … DELETE: the unit-row prefix has zero ops.
        let ops = vec![create_op(), delete_op()];
        let split = split_write_plan(&ops).unwrap();
        assert_eq!(split.prefix_len, 0);
        assert_eq!(split.write_ops, vec![0, 1]);
        assert_eq!(split.read_suffix_start, None);
    }

    #[test]
    fn split_accepts_terminal_return_after_writes() {
        let ops = vec![create_op(), delete_op(), project_op()];
        let split = split_write_plan(&ops).unwrap();
        assert_eq!(split.prefix_len, 0);
        assert_eq!(split.write_ops, vec![0, 1]);
        assert_eq!(split.read_suffix_start, Some(2));
    }

    #[test]
    fn split_tracks_writes_across_intermediate_graph_reads() {
        let ops = vec![scan_op(), create_op(), scan_op(), delete_op()];
        let split = split_write_plan(&ops).unwrap();
        assert_eq!(split.prefix_len, 1);
        assert_eq!(split.write_ops, vec![1, 3]);
        assert_eq!(split.read_suffix_start, None);
    }

    #[test]
    fn split_accepts_merge_mixed_with_other_writes() {
        let ops = vec![scan_op(), merge_op(), delete_op()];
        let split = split_write_plan(&ops).unwrap();
        assert_eq!(split.prefix_len, 1);
        assert_eq!(split.write_ops, vec![1, 2]);
        assert_eq!(split.read_suffix_start, None);
    }

    #[test]
    fn split_errors_on_read_only_plan() {
        let err = split_write_plan(&[scan_op()]).unwrap_err();
        assert!(matches!(err, GfError::Plan(_)), "got {err:?}");
    }

    fn create_pattern_op(created: VarId, source: Option<VarId>) -> GraphOp {
        let mut pattern = CreatePattern {
            nodes: vec![CreateNodeSpec {
                var: created,
                labels: vec![],
                properties: None,
                is_reference: false,
            }],
            edges: vec![],
        };
        if let Some(source) = source {
            pattern.nodes.push(CreateNodeSpec {
                var: source,
                labels: vec![],
                properties: None,
                is_reference: true,
            });
            pattern.edges.push(CreateEdgeSpec {
                var: VarId(created.0 + 100),
                src: source,
                dst: created,
                rel_type: None,
                direction: Direction::Out,
                properties: None,
            });
        }
        GraphOp::Create { pattern }
    }

    #[test]
    fn create_retention_keeps_only_bindings_read_by_later_clauses() {
        let ops = vec![
            create_pattern_op(VarId(0), None),
            create_pattern_op(VarId(1), Some(VarId(0))),
            create_pattern_op(VarId(2), None),
        ];
        let split = split_write_plan(&ops).unwrap();
        let retention = create_retention_by_write(&ops, &ExprArena::new(), &split).unwrap();

        assert_eq!(retention[&0], HashSet::from([VarId(0)]));
        assert!(retention[&1].is_empty());
        assert!(retention[&2].is_empty());
    }

    #[test]
    fn create_retention_is_disabled_across_a_terminal_projection() {
        let ops = vec![create_pattern_op(VarId(0), None), project_op()];
        let split = split_write_plan(&ops).unwrap();
        assert!(create_retention_by_write(&ops, &ExprArena::new(), &split).is_none());
    }

    /// A two-batch frontier with one unqualified Int64 column `x`.
    fn two_batch_frontier() -> Frontier {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3]))],
        )
        .unwrap();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        Frontier {
            df_schema,
            batches: vec![b1, b2],
        }
    }

    #[test]
    fn frontier_append_node_var_resolves_qualified() {
        let mut f = two_batch_frontier();
        assert_eq!(f.num_rows(), 3);

        let uuids = [[1u8; 16], [2u8; 16], [3u8; 16]];
        f.append_node_var(7, &uuids, &[10, 11, 12], &[0, 0, 0])
            .unwrap();

        // Qualified resolution against the logical schema, positional into
        // the physical batches.
        let qual = TableReference::bare("var_7");
        let idx = f
            .df_schema
            .index_of_column_by_name(Some(&qual), "node_uuid")
            .expect("var_7.node_uuid resolves");
        assert_eq!(idx, 1, "appended right after the prefix column");
        for batch in &f.batches {
            assert_eq!(
                batch.num_columns(),
                5,
                "uuid + node_id + primary and complete label ids added"
            );
        }
        // Per-batch row alignment: batch 0 carries rows 0-1, batch 1 row 2.
        let arr = f.batches[1].column(idx);
        let arr = arr
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(arr.value(0), [3u8; 16]);
    }

    #[test]
    fn frontier_append_edge_var_handles_null_rel_names() {
        let mut f = two_batch_frontier();
        let uuids = [[9u8; 16]; 3];
        f.append_edge_var(
            4,
            &uuids,
            &[[1u8; 16]; 3],
            &[[2u8; 16]; 3],
            &[Some("KNOWS".into()), None, Some("KNOWS".into())],
        )
        .unwrap();

        let qual = TableReference::bare("var_4");
        let uuid_idx = f
            .df_schema
            .index_of_column_by_name(Some(&qual), "edge_uuid")
            .expect("var_4.edge_uuid resolves");
        let name_idx = f
            .df_schema
            .index_of_column_by_name(Some(&qual), "rel_type_name")
            .expect("var_4.rel_type_name resolves");
        assert_eq!((uuid_idx, name_idx), (1, 4));
        let names = f.batches[0].column(name_idx);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "KNOWS");
        assert!(names.is_null(1));
    }

    #[test]
    fn frontier_append_on_empty_frontier_is_fine() {
        // Zero-row prefix (e.g. a MATCH with no hits): appends must not
        // choke on empty builders.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let mut f = Frontier {
            df_schema: DFSchema::try_from(schema.as_ref().clone()).unwrap(),
            batches: vec![empty],
        };
        f.append_node_var(1, &[], &[], &[]).unwrap();
        assert_eq!(f.num_rows(), 0);
        assert_eq!(f.batches[0].num_columns(), 5);
    }

    #[test]
    fn terminal_input_materializes_an_empty_batch_when_frontier_has_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let frontier = Frontier {
            df_schema: DFSchema::try_from(schema.as_ref().clone()).unwrap(),
            batches: vec![],
        };

        let (input_schema, batches) = terminal_input(&frontier);
        assert_eq!(input_schema, schema);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    fn nullable_uuids(values: &[Option<[u8; 16]>]) -> ArrayRef {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
        for value in values {
            match value {
                Some(value) => builder.append_value(value).unwrap(),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }

    fn write_frontier(is_edge: bool) -> Frontier {
        let uuid_name = if is_edge { "edge_uuid" } else { "node_uuid" };
        let mut fields = vec![Field::new(uuid_name, DataType::FixedSizeBinary(16), true)];
        let mut columns = vec![nullable_uuids(&[Some([7; 16]), Some([7; 16]), None])];
        if is_edge {
            fields.push(Field::new("rel_type_name", DataType::Utf8, false));
            columns.push(Arc::new(StringArray::from(vec!["KNOWS"; 3])));
        } else {
            fields.push(Field::new("type_id", DataType::UInt32, false));
            columns.push(Arc::new(UInt32Array::from(vec![3; 3])));
        }
        fields.push(Field::new("score", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1)])));
        let schema = Arc::new(Schema::new(fields.clone()));
        let qualifier = TableReference::bare("var_1");
        Frontier {
            df_schema: DFSchema::new_with_metadata(
                fields
                    .into_iter()
                    .map(|field| (Some(qualifier.clone()), Arc::new(field)))
                    .collect(),
                HashMap::new(),
            )
            .unwrap(),
            batches: vec![RecordBatch::try_new(schema, columns).unwrap()],
        }
    }

    fn phase_env<'a>(
        lowerer: &'a GraphPlanLowerer<'a>,
        exprs: &'a ExprArena,
        dir: &'a Path,
        params: &'a HashMap<String, IrLiteral>,
    ) -> PhaseEnv<'a> {
        PhaseEnv {
            lowerer,
            exprs,
            dir,
            mode: OntologyMode::Exploratory,
            params,
            type_map: HashMap::new(),
        }
    }

    #[test]
    fn set_accumulates_nodes_and_edges_once_across_duplicate_and_null_rows() {
        for is_edge in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let lowerer =
                GraphPlanLowerer::new_for_writes(None, None, dir.path(), OntologyMode::Exploratory);
            let mut exprs = ExprArena::new();
            let value = exprs.push(IrExpr::Literal(IrLiteral::Int(42)));
            let params = HashMap::new();
            let env = phase_env(&lowerer, &exprs, dir.path(), &params);
            let mut frontier = write_frontier(is_edge);
            let mut var_map = VarMap::new();
            var_map.insert(VarId(1), "var_1");
            let mut ctx =
                StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();

            run_set_phase(
                &env,
                &[SetPropItem {
                    target: VarId(1),
                    prop: PropId(9),
                    prop_name: "score".into(),
                    value,
                }],
                &mut frontier,
                &var_map,
                &mut ctx,
            )
            .unwrap();

            let accumulated = if is_edge {
                &ctx.set_acc.edges["KNOWS"]
            } else {
                &ctx.set_acc.nodes["_untyped"]
            };
            assert_eq!(
                accumulated[&[7; 16]]["score"],
                IrLiteral::Int(42),
                "wrong accumulated value for is_edge={is_edge}"
            );
            assert_eq!(accumulated.len(), 1, "duplicate rows must coalesce");
            assert_eq!(ctx.counters.properties_set, 1);
            assert_eq!(ctx.counters.properties_removed, 1);
        }
    }

    #[test]
    fn remove_accumulates_nodes_and_edges_and_ignores_null_optional_rows() {
        for is_edge in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let lowerer =
                GraphPlanLowerer::new_for_writes(None, None, dir.path(), OntologyMode::Exploratory);
            let exprs = ExprArena::new();
            let params = HashMap::new();
            let env = phase_env(&lowerer, &exprs, dir.path(), &params);
            let mut frontier = write_frontier(is_edge);
            frontier.batches[0] = RecordBatch::try_new(
                Arc::clone(frontier.batches[0].schema_ref()),
                vec![
                    nullable_uuids(&[Some([7; 16]), Some([8; 16]), None]),
                    Arc::clone(frontier.batches[0].column(1)),
                    Arc::clone(frontier.batches[0].column(2)),
                ],
            )
            .unwrap();
            let mut ctx =
                StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();

            run_remove_phase(
                &env,
                &[RemovePropItem {
                    target: VarId(1),
                    prop: PropId(9),
                    prop_name: "score".into(),
                }],
                &mut frontier,
                &mut ctx,
            )
            .unwrap();

            let accumulated = if is_edge {
                &ctx.remove_acc.edges["KNOWS"]
            } else {
                &ctx.remove_acc.nodes["_untyped"]
            };
            assert_eq!(accumulated[&[7; 16]], HashSet::from(["score".into()]));
            assert_eq!(accumulated[&[8; 16]], HashSet::from(["score".into()]));
            assert_eq!(accumulated.len(), 2);
            assert_eq!(ctx.counters.properties_removed, 2);
        }
    }

    #[test]
    fn set_and_remove_reject_malformed_identity_and_edge_routing_columns() {
        let dir = tempfile::tempdir().unwrap();
        let lowerer =
            GraphPlanLowerer::new_for_writes(None, None, dir.path(), OntologyMode::Exploratory);
        let mut exprs = ExprArena::new();
        let value = exprs.push(IrExpr::Literal(IrLiteral::Int(42)));
        let params = HashMap::new();
        let env = phase_env(&lowerer, &exprs, dir.path(), &params);
        let mut var_map = VarMap::new();
        var_map.insert(VarId(1), "var_1");

        let mut malformed_uuid = write_frontier(false);
        malformed_uuid.batches[0] = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::Int64, false),
                Field::new("type_id", DataType::UInt32, false),
                Field::new("score", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::clone(malformed_uuid.batches[0].column(1)),
                Arc::clone(malformed_uuid.batches[0].column(2)),
            ],
        )
        .unwrap();
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
        let err = run_set_phase(
            &env,
            &[SetPropItem {
                target: VarId(1),
                prop: PropId(9),
                prop_name: "score".into(),
                value,
            }],
            &mut malformed_uuid,
            &var_map,
            &mut ctx,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "execution error: expected FixedSizeBinary(16) at column 0"
        );
        assert!(ctx.set_acc.nodes.is_empty());
        assert!(ctx.set_acc.edges.is_empty());
        assert_eq!(ctx.counters, WriteCounters::default());

        let mut malformed_route = write_frontier(true);
        malformed_route.batches[0] = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("rel_type_name", DataType::Int64, false),
                Field::new("score", DataType::Int64, true),
            ])),
            vec![
                Arc::clone(malformed_route.batches[0].column(0)),
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::clone(malformed_route.batches[0].column(2)),
            ],
        )
        .unwrap();
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
        let err = run_remove_phase(
            &env,
            &[RemovePropItem {
                target: VarId(1),
                prop: PropId(9),
                prop_name: "score".into(),
            }],
            &mut malformed_route,
            &mut ctx,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "execution error: rel_type_name is not a string column"
        );
        assert!(ctx.remove_acc.nodes.is_empty());
        assert!(ctx.remove_acc.edges.is_empty());
        assert_eq!(ctx.counters, WriteCounters::default());
    }
}
