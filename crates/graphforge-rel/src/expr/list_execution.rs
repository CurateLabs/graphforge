//! List quantifier and comprehension execution, including embedded expression rewriting.

use super::value_semantics::validate_heterogeneous_arguments;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// cypher_quantifier UDF (all/any/none/single)
// ---------------------------------------------------------------------------

/// Fold per-element predicate results with three-valued logic per quantifier
/// (#955). `n` is the element count; `bools[j]` is the predicate on element `j`
/// (null = unknown). Empty list: all/none → true, any/single → false.
#[allow(
    clippy::match_same_arms,
    reason = "per-quantifier arms read clearest grouped by kind, even where two \
              fallback bodies coincide (all/none both default true)"
)]
pub(super) fn reduce_quantifier(
    kind: graphforge_ir::QuantifierKind,
    bools: &datafusion::arrow::array::BooleanArray,
    n: usize,
) -> Option<bool> {
    use datafusion::arrow::array::Array;
    use graphforge_ir::QuantifierKind as Q;
    let (mut any_true, mut any_false, mut any_null, mut count_true) = (false, false, false, 0u32);
    for j in 0..n {
        if bools.is_null(j) {
            any_null = true;
        } else if bools.value(j) {
            any_true = true;
            count_true += 1;
        } else {
            any_false = true;
        }
    }
    // Three-valued logic: an unknown (null) element only matters when no
    // definitive element already settles the result.
    match kind {
        Q::All if any_false => Some(false),
        Q::All => (!any_null).then_some(true),
        Q::Any if any_true => Some(true),
        Q::Any => (!any_null).then_some(false),
        Q::None if any_true => Some(false),
        Q::None => (!any_null).then_some(true),
        Q::Single if count_true > 1 => Some(false),
        Q::Single => (!any_null).then_some(count_true == 1),
    }
}

/// `all/any/none/single(loop_var IN list WHERE predicate)` (#955). Holds the
/// predicate as a logical `Expr` over a synthetic element column + the outer
/// columns it references; at invoke time it builds a per-element `RecordBatch`,
/// evaluates the predicate, and folds with [`reduce_quantifier`]. Returns `Boolean`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherQuantifier {
    kind: graphforge_ir::QuantifierKind,
    pub(super) predicate: DfExpr,
    pub(super) elem_name: String,
    pub(super) outer_names: Vec<String>,
    signature: Signature,
}

impl CypherQuantifier {
    pub(super) fn new(
        kind: graphforge_ir::QuantifierKind,
        predicate: DfExpr,
        elem_name: String,
        outer_names: Vec<String>,
    ) -> Self {
        let arity = 1 + outer_names.len();
        Self {
            kind,
            predicate,
            elem_name,
            outer_names,
            // The predicate is embedded in the UDF rather than represented as
            // a call argument and may itself be volatile.
            signature: Signature::any(arity, Volatility::Volatile),
        }
    }
}

/// A quantifier whose predicate is statically true, false, or null. The input
/// list is still evaluated eagerly by DataFusion, but no per-element predicate
/// batch is needed; only list nullability and cardinality affect the result.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherInvariantQuantifier {
    kind: graphforge_ir::QuantifierKind,
    pub(super) predicate: Option<bool>,
    signature: Signature,
}

#[cfg(test)]
pub(super) static INVARIANT_QUANTIFIER_ROWS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl CypherInvariantQuantifier {
    pub(super) fn new(kind: graphforge_ir::QuantifierKind, predicate: Option<bool>) -> Self {
        Self {
            kind,
            predicate,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

pub(super) fn reduce_invariant_quantifier(
    kind: graphforge_ir::QuantifierKind,
    predicate: Option<bool>,
    len: usize,
) -> Option<bool> {
    use graphforge_ir::QuantifierKind as Q;
    if len == 0 {
        return Some(matches!(kind, Q::All | Q::None));
    }
    match (kind, predicate) {
        (_, None) => None,
        (Q::All | Q::Any, Some(value)) => Some(value),
        (Q::None, Some(value)) => Some(!value),
        (Q::Single, Some(true)) => Some(len == 1),
        (Q::Single, Some(false)) => Some(false),
    }
}

impl ScalarUDFImpl for CypherInvariantQuantifier {
    fn name(&self) -> &'static str {
        "cypher_invariant_quantifier"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, BooleanArray, ListArray};
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        #[cfg(test)]
        INVARIANT_QUANTIFIER_ROWS.fetch_add(rows, std::sync::atomic::Ordering::SeqCst);
        let list = args.args[0].to_array(rows)?;
        let list = list.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Internal("cypher_invariant_quantifier: argument is not a list".into())
        })?;
        let values = (0..rows).map(|row| {
            if list.is_null(row) {
                None
            } else {
                reduce_invariant_quantifier(self.kind, self.predicate, list.value(row).len())
            }
        });
        Ok(ColumnarValue::Array(Arc::new(
            values.collect::<BooleanArray>(),
        )))
    }
}

impl ScalarUDFImpl for CypherQuantifier {
    fn name(&self) -> &'static str {
        "cypher_quantifier"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, ListArray, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::error::DataFusionError;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::physical_expr::create_physical_expr;
        use std::sync::Arc;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.to_array(rows))
            .collect::<datafusion::error::Result<_>>()?;
        let list = cols[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("cypher_quantifier: first argument is not a list".into())
            })?;
        let elem_type = match list.data_type() {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            _ => DataType::Null,
        };

        // Synthetic schema: the element column + each referenced outer column.
        let mut fields = vec![Field::new(&self.elem_name, elem_type, true)];
        for (i, name) in self.outer_names.iter().enumerate() {
            fields.push(Field::new(name, cols[i + 1].data_type().clone(), true));
        }
        let schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        // Deferred build: a predicate that cannot plan over the element type
        // (e.g. `x.a = 2` against the Int64 default of a statically-empty
        // list) only errors if a non-empty row actually evaluates it — every
        // empty row short-circuits to the fold identity below.
        let phys = create_physical_expr(&self.predicate, &df_schema, &ExecutionProps::new());

        let mut out = BooleanArray::builder(rows);
        for row in 0..rows {
            if list.is_null(row) {
                out.append_null(); // a quantifier over a null list is null
                continue;
            }
            let elems = list.value(row);
            let n = elems.len();
            if n == 0 {
                // The n = 0 fold yields the identity (`all`/`none` → true,
                // `any`/`single` → false) without running the predicate, whose
                // type over an empty list is irrelevant.
                out.append_option(reduce_quantifier(self.kind, &BooleanArray::new_null(0), 0));
                continue;
            }
            let phys = phys
                .as_ref()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let verdict = (|| -> datafusion::error::Result<Option<bool>> {
                let mut batch_cols: Vec<ArrayRef> = Vec::with_capacity(1 + self.outer_names.len());
                batch_cols.push(elems);
                for i in 0..self.outer_names.len() {
                    let sv = ScalarValue::try_from_array(&cols[i + 1], row)?;
                    batch_cols.push(sv.to_array_of_size(n)?);
                }
                let batch = RecordBatch::try_new(Arc::clone(&schema), batch_cols)?;
                let evaluated = phys.evaluate(&batch)?.into_array(n)?;
                // A typeless evaluation (`WHERE x` over untyped elements) is
                // 3VL unknown per element, not a row failure.
                if evaluated.data_type() == &DataType::Null {
                    return Ok(reduce_quantifier(self.kind, &BooleanArray::new_null(n), n));
                }
                let Some(bools) = evaluated.as_any().downcast_ref::<BooleanArray>() else {
                    return Ok(None);
                };
                Ok(reduce_quantifier(self.kind, bools, n))
            })()?;
            out.append_option(verdict);
        }
        Ok(ColumnarValue::Array(std::sync::Arc::new(out.finish())))
    }
}

/// `[loop_var IN list WHERE filter | projection]` (#955). Holds the optional
/// filter + projection as logical `Expr`s over a synthetic element column +
/// referenced outer columns. At invoke time it builds a per-element
/// `RecordBatch` per row, keeps the elements the filter accepts (3VL: only
/// definitively-true), maps them through the projection, and reassembles a
/// `ListArray`. A bare `[x IN list]` (no clauses) is the list itself; a null
/// list row yields a null list.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherListComp {
    filter: Option<DfExpr>,
    projection: Option<DfExpr>,
    pub(super) elem_name: String,
    pub(super) outer_names: Vec<String>,
    signature: Signature,
}

impl CypherListComp {
    pub(super) fn new(
        filter: Option<DfExpr>,
        projection: Option<DfExpr>,
        elem_name: String,
        outer_names: Vec<String>,
    ) -> Self {
        let arity = 1 + outer_names.len();
        Self {
            filter,
            projection,
            elem_name,
            outer_names,
            // Filter/projection expressions are embedded in the UDF and may
            // contain rand() or another volatile function.
            signature: Signature::any(arity, Volatility::Volatile),
        }
    }

    /// The element type of the result list: the projection's output type over
    /// the synthetic schema, or the input element type when there is no
    /// projection. Computed identically at plan time (`return_type`) and invoke
    /// time so the produced `ListArray`'s child type matches the declared type.
    fn item_type(
        &self,
        elem_type: &DataType,
        outer_types: &[DataType],
    ) -> datafusion::error::Result<DataType> {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::physical_expr::create_physical_expr;
        let Some(proj) = &self.projection else {
            return Ok(elem_type.clone());
        };
        let mut fields = vec![Field::new(&self.elem_name, elem_type.clone(), true)];
        for (name, dt) in self.outer_names.iter().zip(outer_types) {
            fields.push(Field::new(name, dt.clone(), true));
        }
        let schema = Schema::new(fields);
        let df_schema = DFSchema::try_from(schema.clone())?;
        let phys = create_physical_expr(proj, &df_schema, &ExecutionProps::new())?;
        phys.data_type(&schema)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one flatten/filter/project/reassemble pass keeps volatile evaluation and row offsets aligned"
    )]
    fn invoke_uncorrelated(
        list: &datafusion::arrow::array::ListArray,
        schema: datafusion::arrow::datatypes::SchemaRef,
        filter_phys: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        proj_phys: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        item_type: &DataType,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{
            Array, ArrayRef, BooleanArray, ListArray, UInt32Array, new_empty_array,
        };
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::compute::{cast, filter_record_batch, take};
        use datafusion::arrow::datatypes::Field;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::error::DataFusionError;

        let rows = list.len();
        let mut validity = Vec::with_capacity(rows);
        let mut lengths = Vec::with_capacity(rows);
        let mut indices = Vec::new();
        let offsets = list.value_offsets();
        for row in 0..rows {
            let valid = list.is_valid(row);
            validity.push(valid);
            let length = if valid {
                usize::try_from(offsets[row + 1] - offsets[row]).map_err(|_| {
                    DataFusionError::Internal("negative list-comprehension length".into())
                })?
            } else {
                0
            };
            lengths.push(length);
            if valid {
                for index in offsets[row]..offsets[row + 1] {
                    indices.push(u32::try_from(index).map_err(|_| {
                        DataFusionError::Internal(
                            "list-comprehension element index exceeds u32::MAX".into(),
                        )
                    })?);
                }
            }
        }

        let flat: ArrayRef = if indices.len() == list.values().len()
            && indices.first().is_none_or(|first| *first == 0)
        {
            Arc::clone(list.values())
        } else {
            take(list.values(), &UInt32Array::from(indices), None)?
        };
        let total = flat.len();
        let batch = RecordBatch::try_new(schema, vec![flat])?;
        let mask = if let Some(filter) = filter_phys
            && total > 0
        {
            let evaluated = filter.evaluate(&batch)?.into_array(total)?;
            let evaluated = evaluated
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "cypher_list_comprehension: filter did not evaluate to boolean".into(),
                    )
                })?;
            Some(
                (0..total)
                    .map(|index| evaluated.is_valid(index) && evaluated.value(index))
                    .collect::<BooleanArray>(),
            )
        } else {
            None
        };
        let kept = if let Some(mask) = &mask {
            filter_record_batch(&batch, mask)?
        } else {
            batch
        };
        let kept_rows = kept.num_rows();
        let projected = if let Some(projection) = proj_phys {
            if kept_rows == 0 {
                new_empty_array(item_type)
            } else {
                projection.evaluate(&kept)?.into_array(kept_rows)?
            }
        } else {
            Arc::clone(kept.column(0))
        };
        let projected = if projected.data_type() == item_type {
            projected
        } else {
            cast(&projected, item_type)?
        };

        let mut output_offsets = Vec::with_capacity(rows + 1);
        output_offsets.push(0i32);
        let mut input_offset = 0usize;
        let mut output_offset = 0i32;
        for length in lengths {
            let kept = mask.as_ref().map_or(length, |mask| {
                (input_offset..input_offset + length)
                    .filter(|index| mask.value(*index))
                    .count()
            });
            input_offset += length;
            let kept = i32::try_from(kept).map_err(|_| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: list length exceeds i32::MAX".into(),
                )
            })?;
            output_offset = output_offset.checked_add(kept).ok_or_else(|| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: total list length exceeds i32::MAX".into(),
                )
            })?;
            output_offsets.push(output_offset);
        }
        let list = ListArray::try_new(
            Arc::new(Field::new("item", item_type.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(output_offsets)),
            projected,
            Some(NullBuffer::from(validity)),
        )?;
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

impl ScalarUDFImpl for CypherListComp {
    fn name(&self) -> &'static str {
        "cypher_list_comprehension"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        use datafusion::arrow::datatypes::Field;
        // Cypher lists lower to Arrow `List` (i32 offsets), which is also what
        // this UDF produces — keep input handling aligned with `invoke` (which
        // downcasts to `ListArray`) so a `LargeList` cannot pass planning and
        // then fail at runtime.
        let elem_type = match arg_types.first() {
            Some(DataType::List(f)) => f.data_type().clone(),
            _ => DataType::Null,
        };
        let item = self.item_type(&elem_type, arg_types.get(1..).unwrap_or(&[]))?;
        Ok(DataType::List(std::sync::Arc::new(Field::new(
            "item", item, true,
        ))))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the per-row filter/project/reassemble loop reads clearest inline"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{
            Array, ArrayRef, BooleanArray, ListArray, RecordBatch, new_empty_array,
        };
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::compute::{cast, concat, filter_record_batch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::error::DataFusionError;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::physical_expr::create_physical_expr;
        use std::sync::Arc;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.to_array(rows))
            .collect::<datafusion::error::Result<_>>()?;
        let list = cols[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: first argument is not a list".into(),
                )
            })?;
        let elem_type = match list.data_type() {
            DataType::List(f) => f.data_type().clone(),
            _ => DataType::Null,
        };
        let outer_types: Vec<DataType> = (0..self.outer_names.len())
            .map(|i| cols[i + 1].data_type().clone())
            .collect();
        let item_type = self.item_type(&elem_type, &outer_types)?;

        // Synthetic schema: the element column + each referenced outer column.
        let mut fields = vec![Field::new(&self.elem_name, elem_type, true)];
        for (i, name) in self.outer_names.iter().enumerate() {
            fields.push(Field::new(name, cols[i + 1].data_type().clone(), true));
        }
        let schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let props = ExecutionProps::new();
        let filter_phys = self
            .filter
            .as_ref()
            .map(|f| create_physical_expr(f, &df_schema, &props))
            .transpose()?;
        let proj_phys = self
            .projection
            .as_ref()
            .map(|p| create_physical_expr(p, &df_schema, &props))
            .transpose()?;

        if self.outer_names.is_empty() {
            return Self::invoke_uncorrelated(
                list,
                schema,
                filter_phys.as_ref(),
                proj_phys.as_ref(),
                &item_type,
            );
        }

        let mut pieces: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        offsets.push(0);
        let mut validity: Vec<bool> = Vec::with_capacity(rows);
        let mut cur: i32 = 0;

        for row in 0..rows {
            if list.is_null(row) {
                validity.push(false);
                offsets.push(cur);
                continue;
            }
            validity.push(true);
            let elems = list.value(row);
            let n = elems.len();
            let mut batch_cols: Vec<ArrayRef> = Vec::with_capacity(1 + self.outer_names.len());
            batch_cols.push(elems);
            for i in 0..self.outer_names.len() {
                let sv = ScalarValue::try_from_array(&cols[i + 1], row)?;
                batch_cols.push(sv.to_array_of_size(n)?);
            }
            let batch = RecordBatch::try_new(Arc::clone(&schema), batch_cols)?;

            // Filter: keep only elements the predicate accepts (3VL → null/false drop).
            let kept = if let Some(fp) = &filter_phys {
                let mask = fp.evaluate(&batch)?.into_array(n)?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "cypher_list_comprehension: filter did not evaluate to boolean".into(),
                        )
                    })?;
                let clean: BooleanArray =
                    (0..n).map(|j| mask.is_valid(j) && mask.value(j)).collect();
                filter_record_batch(&batch, &clean)?
            } else {
                batch
            };

            // Projection: map each surviving element (or the element itself).
            let projected: ArrayRef = if let Some(pp) = &proj_phys {
                let m = kept.num_rows();
                if m == 0 {
                    new_empty_array(&item_type)
                } else {
                    pp.evaluate(&kept)?.into_array(m)?
                }
            } else {
                Arc::clone(kept.column(0))
            };
            let projected = if projected.data_type() == &item_type {
                projected
            } else {
                cast(&projected, &item_type)?
            };

            let len = i32::try_from(projected.len()).map_err(|_| {
                DataFusionError::Internal("cypher_list_comprehension: list too long".into())
            })?;
            cur = cur.checked_add(len).ok_or_else(|| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: total list length exceeds i32::MAX".into(),
                )
            })?;
            offsets.push(cur);
            pieces.push(projected);
        }

        let child: ArrayRef = if pieces.is_empty() {
            new_empty_array(&item_type)
        } else {
            let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
            concat(&refs)?
        };
        let field = Arc::new(Field::new("item", item_type, true));
        let list_arr = ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            child,
            Some(NullBuffer::from(validity)),
        )?;
        Ok(ColumnarValue::Array(Arc::new(list_arr)))
    }
}

/// Rewrite expressions including the private expression trees retained by
/// quantifiers and list comprehensions. The callback owns resource decisions;
/// this traversal performs no storage admission or I/O.
pub fn rewrite_embedded_expressions(
    expr: DfExpr,
    rewrite: &mut impl FnMut(DfExpr) -> datafusion::common::Result<DfExpr>,
) -> datafusion::common::Result<DfExpr> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    expr.transform_up(|mut expr| {
        if let DfExpr::ScalarFunction(call) = &mut expr {
            if let Some(quantifier) = call.func.inner().downcast_ref::<CypherQuantifier>() {
                call.func = Arc::new(ScalarUDF::new_from_impl(CypherQuantifier::new(
                    quantifier.kind,
                    rewrite_embedded_expressions(quantifier.predicate.clone(), rewrite)?,
                    quantifier.elem_name.clone(),
                    quantifier.outer_names.clone(),
                )));
            } else if let Some(comp) = call.func.inner().downcast_ref::<CypherListComp>() {
                call.func = Arc::new(ScalarUDF::new_from_impl(CypherListComp::new(
                    comp.filter
                        .clone()
                        .map(|expr| rewrite_embedded_expressions(expr, rewrite))
                        .transpose()?,
                    comp.projection
                        .clone()
                        .map(|expr| rewrite_embedded_expressions(expr, rewrite))
                        .transpose()?,
                    comp.elem_name.clone(),
                    comp.outer_names.clone(),
                )));
            }
        }
        rewrite(expr).map(Transformed::yes)
    })
    .map(|result| result.data)
}
