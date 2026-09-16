//! OPTIONAL MATCH and UNWIND physical execution.

use crate::to_df_err;
use arrow::array::Array;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr as DfExpr;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::create_physical_expr;
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
use graphforge_core::GfError;
use graphforge_plan::OptionalMatchNode;
use graphforge_plan::UnwindNode;
use std::fmt;
use std::sync::Arc;

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
pub(super) struct OptionalConfig {
    pub(super) join_keys: Vec<(usize, usize)>,
    /// Inner column indices to append (in order); every shared-variable column
    /// already excluded. Source of truth shared with the node's output schema.
    pub(super) inner_keep_idx: Vec<usize>,
    pub(super) out_schema: SchemaRef,
    /// Outer child's schema — used for `concat_batches` so an empty outer
    /// stream still yields a correctly-typed (zero-row) batch.
    pub(super) outer_schema: SchemaRef,
    /// Inner child's schema — used so an empty inner stream null-shapes (rather
    /// than erroring) instead of having no schema to concat against.
    pub(super) inner_schema: SchemaRef,
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
pub(super) fn optional_join(
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

#[cfg(test)]
mod tests;
