//! Clause-ordered write statement driver (#792, unified for single writes by
//! #817) — the plan
//! split/validation, the shared per-statement write context, and the frontier
//! extension that lets later clauses target entities created by earlier ones.
//!
//! A write statement has exactly one **read prefix** (everything before
//! the first write op), a suffix of **write ops in clause order**, and may end
//! with a terminal `RETURN` projection over the final frontier. Graph reads
//! between writes use the shared pending-write context. The prefix runs once,
//! its rows materialize as the [`Frontier`],
//! and each write clause consumes that same frontier — extended in place with
//! the variables CREATE clauses mint, so a later `DELETE`/`SET`/`REMOVE`
//! resolves them like any matched column.
//!
//! All effects stage during the phase loop (CREATE in the shared writer's
//! buffer, deletions in pending sets, SET/REMOVE in accumulators) and hit
//! disk in **one** [`RewriteBatch`](graphforge_storage::RewriteBatch) commit at
//! statement end — a failure in any phase aborts with the prior on-disk
//! state fully intact.

mod create_merge;
mod mutation_phases;

use create_merge::MatchedMergeEdge;
use create_merge::MatchedMergeNode;
use create_merge::run_create_phase;
use create_merge::run_merge_phase;
use mutation_phases::run_delete_phase;
use mutation_phases::run_label_phase;
use mutation_phases::run_remove_phase;
use mutation_phases::run_set_map_phase;
use mutation_phases::run_set_phase;

use crate::mutation::WriteCounters;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::Int64Array;
use arrow::array::ListArray;
use arrow::array::RecordBatch;
use arrow::array::StringBuilder;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::UInt32Type;
use datafusion::common::Column;
use datafusion::common::DFSchema;

use datafusion::logical_expr::Expr as DfExpr;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;

use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_ir::CreatePattern;
use graphforge_ir::ExprArena;
use graphforge_ir::ExprId;
use graphforge_ir::IrExpr;
use graphforge_ir::VarId;
use graphforge_ir::plan::GraphOp;
use graphforge_plan::ResolvedEdgeSpec;
use graphforge_plan::ResolvedNodeSpec;

use graphforge_rel::GraphPlanLowerer;
use graphforge_rel::VarMap;
use graphforge_value::EntityTypeId;

use crate::RemoveAccumulator;
use crate::SetAccumulator;

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
    pub label_additions: HashMap<[u8; 16], HashSet<EntityTypeId>>,
    pub label_removals: HashMap<[u8; 16], HashSet<EntityTypeId>>,
    /// Label tokens already present before, or introduced during, this statement.
    pub known_labels: HashSet<EntityTypeId>,
    pub removed_label_tokens: HashSet<EntityTypeId>,
    pub mutation: crate::mutation::MutationState,
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
                    known_labels.extend(decode_memberships(values)?);
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
            mutation: crate::mutation::MutationState::default(),
        })
    }

    #[must_use]
    pub(crate) fn with_semantic_composition_fingerprint(
        mut self,
        fingerprint: Option<String>,
    ) -> Self {
        self.writer = self
            .writer
            .with_semantic_composition_fingerprint(fingerprint);
        self
    }

    fn record_mutation_input(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.mutation
            .record_mutation_input(kind, subject_kind, uuid);
    }

    fn record_mutation_output(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.mutation
            .record_mutation_output(kind, subject_kind, uuid);
    }

    pub(crate) fn mutation_receipt(&self) -> crate::MutationReceipt {
        self.mutation.mutation_receipt()
    }

    fn record_label_tokens(&mut self, labels: impl IntoIterator<Item = EntityTypeId>) {
        for label in labels {
            if self.removed_label_tokens.remove(&label) {
                self.mutation.counters.labels_removed -= 1;
            }
            if self.known_labels.insert(label) {
                self.mutation.counters.labels_added += 1;
            }
        }
    }

    fn record_property_set(&mut self, is_edge: bool, uuid: [u8; 16], name: &str) -> bool {
        self.mutation.record_property_set(is_edge, uuid, name)
    }

    fn record_removed_label_tokens(&mut self, labels: impl IntoIterator<Item = EntityTypeId>) {
        for label in labels {
            if self.removed_label_tokens.insert(label) {
                self.mutation.counters.labels_removed += 1;
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

#[derive(Clone, Copy)]
enum LabelRewrite {
    Add,
    Remove,
}

impl LabelRewrite {
    const fn missing_column_error(self) -> &'static str {
        match self {
            Self::Add => "MERGE label target has no type_ids column",
            Self::Remove => "REMOVE label target has no type_ids column",
        }
    }

    fn apply(self, current: &mut Vec<EntityTypeId>, labels: &[EntityTypeId]) {
        match self {
            Self::Add => {
                current.extend_from_slice(labels);
                current.sort_unstable_by_key(|id| id.encode());
                current.dedup();
            }
            Self::Remove => current.retain(|label| !labels.contains(label)),
        }
    }
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
            .map_err(GfError::from_execution_error)?;
        let indices = UInt64Array::from(indices.to_vec());
        let columns = input
            .columns()
            .iter()
            .map(|column| {
                arrow::compute::take(column, &indices, None).map_err(GfError::from_execution_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.batches = vec![
            RecordBatch::try_new_with_options(
                schema,
                columns,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
            )
            .map_err(GfError::from_execution_error)?,
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
                    .map_err(GfError::from_execution_error)?,
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
            .map_err(GfError::from_execution_error)?;
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
                    .map_err(GfError::from_execution_error)?;
            }
            Ok(vec![
                Arc::new(uuid_b.finish()) as ArrayRef,
                Arc::new(UInt64Array::from(node_ids[range.clone()].to_vec())),
                Arc::new(UInt32Array::from(type_ids[range.clone()].to_vec())),
                non_null_label_items(&ListArray::from_iter_primitive::<UInt32Type, _, _>(
                    type_ids[range].iter().map(|id| Some([Some(*id)])),
                )),
            ])
        })
    }

    pub(crate) fn append_created_node_var(
        &mut self,
        spec: &ResolvedNodeSpec,
        uuids: &[[u8; 16]],
        node_ids: &[u64],
        type_ids: &[graphforge_value::PrimaryEntityTypeId],
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
                    .map_err(GfError::from_execution_error)?;
            }
            let mut cols: Vec<ArrayRef> = vec![
                Arc::new(uuid_b.finish()) as ArrayRef,
                Arc::new(UInt64Array::from(node_ids[range.clone()].to_vec())),
                Arc::new(UInt32Array::from(
                    type_ids[range]
                        .iter()
                        .map(|id| id.encode())
                        .collect::<Vec<_>>(),
                )),
                repeated_label_sets(&spec.label_ids, rows),
            ];
            for (_, lit) in &spec.properties {
                let scalar = graphforge_rel::expr::ir_literal_to_scalar(lit);
                cols.push(
                    scalar
                        .to_array_of_size(rows)
                        .map_err(GfError::from_execution_error)?,
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
                    .map_err(GfError::from_execution_error)?;
            }
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(uuid_builder.finish()),
                Arc::new(UInt64Array::from(
                    selected.iter().map(|row| row.node_id).collect::<Vec<_>>(),
                )),
                Arc::new(UInt32Array::from(
                    selected
                        .iter()
                        .map(|row| row.type_id.encode())
                        .collect::<Vec<_>>(),
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
                    .map_err(GfError::from_execution_error)?;
                columns.push(
                    ScalarValue::iter_to_array(values).map_err(GfError::from_execution_error)?,
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
            .map_err(GfError::from_execution_error)?;

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
                    .map_err(GfError::from_execution_error)
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
                    .map_err(GfError::from_execution_error)?;
                src_b
                    .append_value(src_uuids[row])
                    .map_err(GfError::from_execution_error)?;
                dst_b
                    .append_value(dst_uuids[row])
                    .map_err(GfError::from_execution_error)?;
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
                    .map_err(GfError::from_execution_error)?;
                src_b
                    .append_value(identities.src_uuids[row])
                    .map_err(GfError::from_execution_error)?;
                dst_b
                    .append_value(identities.dst_uuids[row])
                    .map_err(GfError::from_execution_error)?;
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
                        .map_err(GfError::from_execution_error)?,
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
                    .map_err(GfError::from_execution_error)?;
                src_builder
                    .append_value(row.src_uuid)
                    .map_err(GfError::from_execution_error)?;
                dst_builder
                    .append_value(row.dst_uuid)
                    .map_err(GfError::from_execution_error)?;
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
                    .map_err(GfError::from_execution_error)?;
                columns.push(
                    ScalarValue::iter_to_array(values).map_err(GfError::from_execution_error)?,
                );
            }
            Ok(columns)
        })
    }

    fn add_node_labels(
        &mut self,
        var: VarId,
        labels: &[EntityTypeId],
        mask: &[bool],
    ) -> Result<(), GfError> {
        if mask.len() != self.num_rows() {
            return Err(GfError::Execution(
                "MERGE label mask does not match frontier rows".into(),
            ));
        }
        self.rewrite_node_labels(var, labels, mask, LabelRewrite::Add)
    }

    fn remove_node_labels(&mut self, var: VarId, labels: &[EntityTypeId]) -> Result<(), GfError> {
        let mask = vec![true; self.num_rows()];
        self.rewrite_node_labels(var, labels, &mask, LabelRewrite::Remove)
    }

    fn rewrite_node_labels(
        &mut self,
        var: VarId,
        labels: &[EntityTypeId],
        mask: &[bool],
        rewrite: LabelRewrite,
    ) -> Result<(), GfError> {
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", var.0));
        let index = self
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan(rewrite.missing_column_error().into()))?;
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
                let mut rewritten = decode_memberships(values)?;
                if mask[offset + row] {
                    rewrite.apply(&mut rewritten, labels);
                }
                rows.push(Some(
                    rewritten
                        .into_iter()
                        .map(|id| Some(id.encode()))
                        .collect::<Vec<_>>(),
                ));
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
                    .map_err(GfError::from_execution_error)?,
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

pub(crate) fn repeated_label_sets(
    labels: &[graphforge_value::EntityTypeId],
    rows: usize,
) -> ArrayRef {
    let array = ListArray::from_iter_primitive::<UInt32Type, _, _>(
        (0..rows).map(|_| Some(labels.iter().map(|id| Some(id.encode())))),
    );
    non_null_label_items(&array)
}

fn repeated_row_label_sets(rows: &[MatchedMergeNode]) -> ArrayRef {
    let array = ListArray::from_iter_primitive::<UInt32Type, _, _>(
        rows.iter()
            .map(|row| Some(row.label_ids.iter().map(|id| Some(id.encode())))),
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
    type_ids: Vec<graphforge_value::PrimaryEntityTypeId>,
}

type NodeIdentitySlices<'a> = (
    &'a [[u8; 16]],
    &'a [u64],
    &'a [graphforge_value::PrimaryEntityTypeId],
);

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
    pub(crate) fn record_node(
        &mut self,
        var: u32,
        uuid: [u8; 16],
        node_id: u64,
        type_id: graphforge_value::PrimaryEntityTypeId,
    ) {
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
    pub inventory: Option<std::sync::Arc<graphforge_storage::AuthenticatedPropertyInventory>>,
    pub lowerer: &'a GraphPlanLowerer,
    pub exprs: &'a ExprArena,
    pub dir: &'a Path,
    pub mode: OntologyMode,
    pub params: &'a HashMap<String, graphforge_ir::IrLiteral>,
    /// `TypeId.0 → entity name`, for property-file stem resolution.
    pub type_map: HashMap<graphforge_value::EntityTypeId, String>,
    pub hydration: std::sync::Arc<crate::path_hydration::HydrationResource>,
}

impl Drop for PhaseEnv<'_> {
    fn drop(&mut self) {
        self.hydration.cancel();
    }
}

impl PhaseEnv<'_> {
    fn bind_read_expression(&self, expr: DfExpr) -> Result<DfExpr, GfError> {
        self.hydration.bind(expr).map_err(GfError::from_plan_error)
    }
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
    .map_err(GfError::from_plan_error)
}

/// Run the statement's write ops in clause order against the shared frontier
/// and context. Nothing touches disk here — effects buffer in `ctx` and
/// commit in [`stage_statement`].
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
        .map_err(GfError::from_plan_error)?;
    let (input_schema, input_batches) = terminal_input(frontier);
    let input = MemorySourceConfig::try_new_from_batches(input_schema, input_batches)
        .map_err(GfError::from_plan_error)?;
    let physical = replace_empty_input(physical, input)?;
    let schema = physical.schema();
    let mut batches = crate::path_hydration::collect_guarded(physical, session.task_ctx())
        .await
        .map_err(GfError::from_execution_error)?;
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
                    .map_err(GfError::from_plan_error)?;
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
        .map_err(GfError::from_execution_error)
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
    if plan.is::<EmptyExec>() || plan.is::<PlaceholderRowExec>() {
        return Ok(input);
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| replace_empty_input(Arc::clone(child), Arc::clone(&input)))
        .collect::<Result<Vec<_>, _>>()?;
    plan.with_new_children(children)
        .map_err(GfError::from_plan_error)
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
        .map_err(GfError::from_plan_error)?
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
    let schema =
        DFSchema::new_with_metadata(fields, HashMap::new()).map_err(GfError::from_plan_error)?;
    Ok((expr, schema))
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
pub(crate) fn stage_statement(
    ctx: &mut StatementWriteContext,
    dir: &Path,
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
) -> Result<graphforge_storage::RewriteBatch, GfError> {
    // Writes to entities deleted later in the statement are unobservable —
    // they must not resurrect rows in the rewrite.
    ctx.set_acc.scrub(&ctx.deleted);
    ctx.remove_acc.scrub(&ctx.deleted);

    // Property values/counts belong to the writable workspace; the pinned
    // generation contributes only declared semantic owners for absent routes.
    let workspace_inventory = if ctx.set_acc.is_empty() && ctx.remove_acc.is_empty() {
        None
    } else {
        Some(graphforge_storage::AuthenticatedPropertyInventory::capture_workspace(dir, inventory)?)
    };
    let inventory = workspace_inventory.as_ref();
    let mut staged = graphforge_storage::RewriteBatch::new();
    ctx.set_acc.stage_into(&mut staged, dir, inventory)?;
    ctx.remove_acc.stage_into(&mut staged, dir, inventory)?;
    graphforge_storage::stage_mutate_node_labels(
        &mut staged,
        dir,
        &ctx.label_additions,
        &ctx.label_removals,
    )?;
    graphforge_storage::stage_delete_edges(&mut staged, dir, &ctx.pending_edge_deletes)?;
    graphforge_storage::stage_delete_nodes(&mut staged, dir, &ctx.pending_node_deletes)?;
    ctx.writer.flush_into(&mut staged)?;

    Ok(staged)
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
    .map_err(GfError::from_execution_error)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn decode_memberships(values: &UInt32Array) -> Result<Vec<EntityTypeId>, GfError> {
    values
        .iter()
        .map(|value| {
            let encoded =
                value.ok_or_else(|| GfError::Storage("null node membership identity".into()))?;
            EntityTypeId::decode(encoded).map_err(|error| GfError::Storage(error.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests;
