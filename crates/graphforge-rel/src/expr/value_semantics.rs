//! Cypher comparison, equality, membership and three-valued predicates.

use super::{
    date_struct_value, datetime_struct_parts, duration_struct_parts, is_date_struct,
    is_datetime_struct, is_duration_struct, is_het_struct_type, is_localdatetime_struct,
    is_time_struct, localdatetime_struct_parts, time_struct_parts, unwrap_het,
};
use datafusion::arrow::array::{Array, FixedSizeListArray, LargeListArray, ListArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

pub(super) static CYPHER_AND: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherBoolOp::new(CypherBoolOpKind::And)));
pub(super) static CYPHER_OR: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherBoolOp::new(CypherBoolOpKind::Or)));
pub(super) static CYPHER_XOR: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherBoolOp::new(CypherBoolOpKind::Xor)));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CypherBoolOpKind {
    And,
    Or,
    Xor,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherBoolOp {
    signature: Signature,
    kind: CypherBoolOpKind,
}

impl CypherBoolOp {
    fn new(kind: CypherBoolOpKind) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            kind,
        }
    }
}

impl ScalarUDFImpl for CypherBoolOp {
    fn name(&self) -> &'static str {
        match self.kind {
            CypherBoolOpKind::And => "cypher_and",
            CypherBoolOpKind::Or => "cypher_or",
            CypherBoolOpKind::Xor => "cypher_xor",
        }
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
        use datafusion::arrow::array::BooleanArray;
        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i)?;
                let r = ScalarValue::try_from_array(&rhs, i)?;
                let l = scalar_as_bool(&l)?;
                let r = scalar_as_bool(&r)?;
                Ok::<Option<bool>, datafusion::error::DataFusionError>(match self.kind {
                    CypherBoolOpKind::And => match (l, r) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    },
                    CypherBoolOpKind::Or => match (l, r) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    },
                    CypherBoolOpKind::Xor => match (l, r) {
                        (Some(left), Some(right)) => Some(left ^ right),
                        _ => None,
                    },
                })
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?
            .into_iter()
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

fn scalar_as_bool(s: &ScalarValue) -> datafusion::error::Result<Option<bool>> {
    let s = unwrap_het(s.clone());
    if s.is_null() {
        return Ok(None);
    }
    match s {
        ScalarValue::Boolean(v) => Ok(v),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "expected boolean operand, got {other:?}"
        ))),
    }
}

/// The two operands of a Cypher `OR`, when `function` is exactly that UDF.
pub(super) fn cypher_or_operands(
    function: &datafusion::logical_expr::expr::ScalarFunction,
) -> Option<(&DfExpr, &DfExpr)> {
    let op = function.func.inner().downcast_ref::<CypherBoolOp>()?;
    match (op.kind, function.args.as_slice()) {
        (CypherBoolOpKind::Or, [left, right]) => Some((left, right)),
        _ => None,
    }
}

pub(super) static CYPHER_CMP_PRED: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherCmpPred::new()));

pub(super) fn is_comparison_predicate(
    function: &datafusion::logical_expr::expr::ScalarFunction,
) -> bool {
    function
        .func
        .inner()
        .downcast_ref::<CypherCmpPred>()
        .is_some()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherCmpPred {
    signature: Signature,
}

impl CypherCmpPred {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherCmpPred {
    fn name(&self) -> &'static str {
        "cypher_cmp_pred"
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
        use datafusion::arrow::array::BooleanArray;
        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let op = args.args[2].to_array(rows)?;
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i)?;
                let r = ScalarValue::try_from_array(&rhs, i)?;
                let op = ScalarValue::try_from_array(&op, i)?;
                let op = scalar_as_i8(&op)?;
                Ok::<Option<bool>, datafusion::error::DataFusionError>(cypher_compare_pred(
                    &l, &r, op,
                ))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?
            .into_iter()
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

fn scalar_as_i8(s: &ScalarValue) -> datafusion::error::Result<i8> {
    match s {
        ScalarValue::Int8(Some(v)) => Ok(*v),
        ScalarValue::Int64(Some(v)) => i8::try_from(*v).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "comparison opcode {v} is outside i8 range"
            ))
        }),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "comparison opcode must be an integer, got {other:?}"
        ))),
    }
}

fn cypher_compare_pred(l: &ScalarValue, r: &ScalarValue, op: i8) -> Option<bool> {
    let l = unwrap_het(l.clone());
    let r = unwrap_het(r.clone());
    if l.is_null() || r.is_null() {
        return None;
    }
    if is_numeric_scalar(&l) && is_numeric_scalar(&r) {
        let lf = scalar_as_f64(&l)?;
        let rf = scalar_as_f64(&r)?;
        if lf.is_nan() || rf.is_nan() {
            return Some(false);
        }
    }
    let cmp = cypher_compare(&l, &r)?;
    Some(match op {
        0 => cmp < 0,
        1 => cmp <= 0,
        2 => cmp > 0,
        3 => cmp >= 0,
        _ => false,
    })
}

fn is_numeric_scalar(s: &ScalarValue) -> bool {
    scalar_as_f64(s).is_some()
}

pub(crate) static CYPHER_ORDER_KEY: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherOrderKey::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherOrderKey {
    signature: Signature,
}

impl CypherOrderKey {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherOrderKey {
    fn name(&self) -> &'static str {
        "cypher_order_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::StringArray;
        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let out: StringArray = (0..rows)
            .map(|i| {
                let v = ScalarValue::try_from_array(&values, i).ok()?;
                Some(cypher_order_key(&v))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

pub(crate) fn needs_cypher_order_key_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    ) || (matches!(t, DataType::Struct(_))
        && !is_date_struct(t)
        && !is_localdatetime_struct(t)
        && !is_duration_struct(t))
}

pub(super) fn cypher_order_key(v: &ScalarValue) -> String {
    let v = unwrap_het(v.clone());
    if v.is_null() {
        return "99:null".to_string();
    }
    match &v {
        ScalarValue::Struct(s) if is_time_struct(&v.data_type()) => {
            let Some((nanos, offset)) = time_struct_parts(s, 0) else {
                return "99:null".to_string();
            };
            let instant = i128::from(nanos) - i128::from(offset) * 1_000_000_000;
            format!(
                "55:time:{}:{}",
                ordered_i128_key(instant),
                ordered_i128_key(i128::from(offset))
            )
        }
        ScalarValue::Struct(s) if is_datetime_struct(&v.data_type()) => {
            let Some((days, nanos, offset, zone)) = datetime_struct_parts(s, 0) else {
                return "99:null".to_string();
            };
            let instant = i128::from(days) * 86_400_000_000_000 + i128::from(nanos)
                - i128::from(offset) * 1_000_000_000;
            format!(
                "55:datetime:{}:{}:{}",
                ordered_i128_key(instant),
                ordered_i128_key(i128::from(offset)),
                zone.map_or_else(|| "0".to_owned(), |zone| format!("1{zone}"))
            )
        }
        ScalarValue::Struct(s) if is_path_struct(s) => "50:path".to_string(),
        ScalarValue::Struct(s) if is_rel_struct(s) => "30:rel".to_string(),
        ScalarValue::Struct(s) if is_node_struct(s) => "20:node".to_string(),
        ScalarValue::Struct(_) => "10:map".to_string(),
        ScalarValue::List(a) => format!("40:list:{}", cypher_list_order_key(&a.value(0))),
        ScalarValue::LargeList(a) => format!("40:list:{}", cypher_list_order_key(&a.value(0))),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => format!("60:str:{s}"),
        ScalarValue::Boolean(Some(b)) => format!("70:bool:{}", u8::from(*b)),
        ScalarValue::Int64(Some(n)) => {
            #[allow(
                clippy::cast_precision_loss,
                reason = "Cypher numeric order shares a number bucket across ints and floats"
            )]
            let n = *n as f64;
            format!("80:num:{}", ordered_f64_key(n))
        }
        ScalarValue::Float64(Some(f)) if f.is_nan() => "90:nan".to_string(),
        ScalarValue::Float64(Some(f)) => format!("80:num:{}", ordered_f64_key(*f)),
        _ => "98:other".to_string(),
    }
}

fn cypher_list_order_key(values: &datafusion::arrow::array::ArrayRef) -> String {
    let mut out = String::new();
    for i in 0..values.len() {
        let v = ScalarValue::try_from_array(values, i).unwrap_or(ScalarValue::Null);
        out.push_str(&cypher_order_key(&v));
        out.push('|');
    }
    out.push_str("00:end");
    out
}

fn ordered_f64_key(f: f64) -> String {
    let bits = f.to_bits();
    let key = if (bits >> 63) == 0 {
        bits | (1 << 63)
    } else {
        !bits
    };
    format!("{key:016x}")
}

fn ordered_i128_key(value: i128) -> String {
    let key = value.cast_unsigned() ^ (1_u128 << 127);
    format!("{key:032x}")
}

fn is_node_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.column_by_name("node_uuid").is_some() || s.column_by_name("labels").is_some()
}

fn is_rel_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.column_by_name("edge_uuid").is_some()
        || s.column_by_name("src_uuid").is_some()
        || s.column_by_name("dst_uuid").is_some()
}

fn is_path_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.column_by_name("nodes").is_some() || s.column_by_name("relationships").is_some()
}

// ---------------------------------------------------------------------------
// cypher_eq UDF
// ---------------------------------------------------------------------------

/// Cypher equality (`=`; `<>` is its negation). Unlike SQL `=`, comparing two
/// values of **different types** is `false` rather than a planning error, and
/// `null = x` is `null` (three-valued). Same-type and mixed-numeric operands use
/// Arrow's null-propagating `eq` kernel; everything else (string-vs-number,
/// temporal-vs-other, …) is `false` where both operands are non-null. (ADR 0009)
pub(super) static CYPHER_EQ: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherEq::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherEq {
    signature: Signature,
}

impl CypherEq {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherEq {
    fn name(&self) -> &'static str {
        "cypher_eq"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    /// Rewrite to the native `=` so DataFusion keeps its optimizations (filter
    /// pushdown, join-key recognition) whenever the operands are statically the
    /// **same** primitive type, or a type can't be resolved (e.g. a `$param`,
    /// which native `=` coerces and pushes down). The type-tolerant UDF is kept
    /// for everything else: **nested** operands (three-valued structural
    /// equality) and **differing** types — including differing numeric widths
    /// (`UInt64` vs `Int64`), where native `=` not only risks a planning error
    /// but trips DataFusion's interval analysis (`lhs_type == rhs_type`); the
    /// UDF compares those via `f64` at runtime instead.
    fn simplify(
        &self,
        args: Vec<DfExpr>,
        info: &datafusion::logical_expr::simplify::SimplifyContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::simplify::ExprSimplifyResult> {
        use datafusion::logical_expr::simplify::ExprSimplifyResult;
        let [l, r] = args.as_slice() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let nested = |t: &DataType| {
            matches!(
                t,
                DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_)
            )
        };
        let floaty =
            |t: &DataType| matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64);
        // A `$param` placeholder has no statically-fixed type here; native `=`
        // coerces it to the other operand at bind time and pushes down (the
        // common `WHERE prop = $x`), so don't trap it in the UDF.
        let placeholder = |e: &DfExpr| matches!(e, DfExpr::Placeholder(_));
        let keep_udf = if placeholder(l) || placeholder(r) {
            false
        } else if let (Ok(lt), Ok(rt)) = (info.get_data_type(l), info.get_data_type(r)) {
            nested(&lt) || nested(&rt) || floaty(&lt) || floaty(&rt) || lt != rt
        } else {
            false // unresolved operand type(s) ⇒ native `=` handles it (and pushes down)
        };
        if keep_udf {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let [l, r]: [DfExpr; 2] = args.try_into().expect("checked length 2 above");
        Ok(ExprSimplifyResult::Simplified(l.eq(r)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::BooleanArray;
        use datafusion::arrow::compute::kernels::cmp::eq;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let (lt, rt) = (lhs.data_type(), rhs.data_type());

        let nested = |t: &DataType| {
            matches!(
                t,
                DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_)
            )
        };

        let floaty =
            |t: &DataType| matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64);

        // Fast path: same non-float primitive type → Arrow's null-propagating
        // equality kernel (vectorised, and identical to the prior native `=`
        // behaviour). Floats stay on the Cypher path so NaN never equals itself.
        if lt == rt && !nested(lt) && !floaty(lt) {
            let res = eq(&lhs, &rhs)?;
            return Ok(ColumnarValue::Array(std::sync::Arc::new(res)));
        }

        // Float, nested, numeric-coercion and cross-type comparisons need Cypher's three-valued
        // structural equality, which the scalar `=` kernel doesn't provide:
        // compare value-by-value via `ScalarValue`.
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i).ok()?;
                let r = ScalarValue::try_from_array(&rhs, i).ok()?;
                cypher_value_eq(&l, &r)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `x IN list` with Cypher three-valued structural membership (ADR 0011): used
/// when the list is a heterogeneous/nested tagged list, which DataFusion's native
/// `in_list` cannot compare. Decodes each element and reuses [`cypher_value_eq`];
/// a definitive match wins, else any `null` comparison yields `null`, else `false`
/// (an empty list is `false`, even for a `null` left operand).
pub(super) static CYPHER_IN: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherIn::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherIn {
    signature: Signature,
}

impl CypherIn {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

pub(super) enum ListView<'a> {
    Fixed(&'a FixedSizeListArray),
    List(&'a ListArray),
    Large(&'a LargeListArray),
}

impl ListView<'_> {
    pub(super) fn from_array(array: &datafusion::arrow::array::ArrayRef) -> Option<ListView<'_>> {
        if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            Some(ListView::Fixed(list))
        } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
            Some(ListView::List(list))
        } else {
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .map(ListView::Large)
        }
    }

    pub(super) fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Fixed(a) => a.is_null(row),
            Self::List(a) => a.is_null(row),
            Self::Large(a) => a.is_null(row),
        }
    }

    pub(super) fn value(&self, row: usize) -> datafusion::arrow::array::ArrayRef {
        match self {
            Self::Fixed(a) => a.value(row),
            Self::List(a) => a.value(row),
            Self::Large(a) => a.value(row),
        }
    }
}

fn cypher_in_elems(lhs: &ScalarValue, elems: &datafusion::arrow::array::ArrayRef) -> Option<bool> {
    let mut saw_null = false;
    for j in 0..elems.len() {
        let ev = ScalarValue::try_from_array(elems, j).ok()?;
        match cypher_value_eq(lhs, &ev) {
            Some(true) => return Some(true),
            None => saw_null = true,
            Some(false) => {}
        }
    }
    if saw_null { None } else { Some(false) }
}

fn cypher_in_tagged_list(
    lhs: &ScalarValue,
    rhs: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> Option<bool> {
    let rv = ScalarValue::try_from_array(rhs, row).ok()?;
    match unwrap_het(rv) {
        ScalarValue::List(list) => {
            if list.is_null(0) {
                None
            } else {
                cypher_in_elems(lhs, &list.value(0))
            }
        }
        ScalarValue::LargeList(list) => {
            if list.is_null(0) {
                None
            } else {
                cypher_in_elems(lhs, &list.value(0))
            }
        }
        _ => None,
    }
}

impl ScalarUDFImpl for CypherIn {
    fn name(&self) -> &'static str {
        "cypher_in"
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
        use datafusion::arrow::array::BooleanArray;
        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        if is_het_struct_type(Some(rhs.data_type())) {
            let out: BooleanArray = (0..rows)
                .map(|i| {
                    let lv = ScalarValue::try_from_array(&lhs, i).ok()?;
                    cypher_in_tagged_list(&lv, &rhs, i)
                })
                .collect();
            return Ok(ColumnarValue::Array(std::sync::Arc::new(out)));
        }
        let list = if let Some(list) = rhs.as_any().downcast_ref::<FixedSizeListArray>() {
            ListView::Fixed(list)
        } else if let Some(list) = rhs.as_any().downcast_ref::<ListArray>() {
            ListView::List(list)
        } else if let Some(list) = rhs.as_any().downcast_ref::<LargeListArray>() {
            ListView::Large(list)
        } else {
            return Ok(ColumnarValue::Array(std::sync::Arc::new(
                BooleanArray::new_null(rows),
            )));
        };
        let out: BooleanArray = (0..rows)
            .map(|i| {
                if list.is_null(i) {
                    return None; // `x IN null` → null
                }
                let lv = ScalarValue::try_from_array(&lhs, i).ok()?;
                let elems = list.value(i);
                cypher_in_elems(&lv, &elems)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// Cypher order COMPARABILITY of `<`/`<=`/`>`/`>=`: `Some(-1|0|1)` when the two
/// values are comparable, `None` (= `null`) when either is null or the types are
/// not order-comparable (e.g. a list vs a boolean). Numbers compare across
/// `Int`/`Float`; same-typed strings/booleans compare directly; two lists compare
/// lexicographically (a shorter prefix sorts first), with an incomparable element
/// making the whole comparison `null`. Unlike *orderability* (used by min/max),
/// comparability does NOT impose a cross-type order.
fn cypher_compare(a: &ScalarValue, b: &ScalarValue) -> Option<i8> {
    let to_i8 = |o: std::cmp::Ordering| o as i8;
    let a = unwrap_het(a.clone());
    let b = unwrap_het(b.clone());
    if a.is_null() || b.is_null() {
        return None;
    }
    match (&a, &b) {
        _ if is_numeric_scalar(&a) && is_numeric_scalar(&b) => {
            if let (Some(x), Some(y)) = (scalar_as_i128(&a), scalar_as_i128(&b)) {
                Some(to_i8(x.cmp(&y)))
            } else {
                scalar_as_f64(&a)?
                    .partial_cmp(&scalar_as_f64(&b)?)
                    .map(to_i8)
            }
        }
        (ScalarValue::Utf8(Some(x)), ScalarValue::Utf8(Some(y))) => Some(to_i8(x.cmp(y))),
        (ScalarValue::Boolean(Some(x)), ScalarValue::Boolean(Some(y))) => Some(to_i8(x.cmp(y))),
        (ScalarValue::Time64Nanosecond(Some(x)), ScalarValue::Time64Nanosecond(Some(y))) => {
            Some(to_i8(x.cmp(y)))
        }
        (ScalarValue::List(x), ScalarValue::List(y)) => {
            cypher_seq_compare(&x.value(0), &y.value(0))
        }
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_date_struct(&a.data_type()) && is_date_struct(&b.data_type()) =>
        {
            Some(to_i8(
                date_struct_value(x, 0)?.cmp(&date_struct_value(y, 0)?),
            ))
        }
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_localdatetime_struct(&a.data_type())
                && is_localdatetime_struct(&b.data_type()) =>
        {
            Some(to_i8(
                localdatetime_struct_parts(x, 0)?.cmp(&localdatetime_struct_parts(y, 0)?),
            ))
        }
        // `time` orders by its UTC instant (`time - offset`), not the struct's
        // native lexicographic `(time, offset)` (#1008, Temporal7 [3]). Two
        // values at one instant with different offsets are unequal, so they
        // order by offset: `<`, `<=`, `=`, `>=` and `>` then agree (#1887 D11).
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_time_struct(&a.data_type()) && is_time_struct(&b.data_type()) =>
        {
            let (xn, xo) = time_struct_parts(x, 0)?;
            let (yn, yo) = time_struct_parts(y, 0)?;
            let xi = i128::from(xn) - i128::from(xo) * 1_000_000_000;
            let yi = i128::from(yn) - i128::from(yo) * 1_000_000_000;
            Some(to_i8(xi.cmp(&yi).then(xo.cmp(&yo))))
        }
        // `datetime` orders by instant, then offset, then named zone (an
        // offset-only value first): the components equality compares.
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_datetime_struct(&a.data_type()) && is_datetime_struct(&b.data_type()) =>
        {
            let (xd, xn, xo, xz) = datetime_struct_parts(x, 0)?;
            let (yd, yn, yo, yz) = datetime_struct_parts(y, 0)?;
            let xi = i128::from(xd) * 86_400_000_000_000 + i128::from(xn)
                - i128::from(xo) * 1_000_000_000;
            let yi = i128::from(yd) * 86_400_000_000_000 + i128::from(yn)
                - i128::from(yo) * 1_000_000_000;
            Some(to_i8(xi.cmp(&yi).then(xo.cmp(&yo)).then(xz.cmp(&yz))))
        }
        _ => None, // incomparable types
    }
}

/// Lexicographic comparability of two list element arrays (see [`cypher_compare`]).
fn cypher_seq_compare(
    a: &datafusion::arrow::array::ArrayRef,
    b: &datafusion::arrow::array::ArrayRef,
) -> Option<i8> {
    let common = a.len().min(b.len());
    for i in 0..common {
        let av = ScalarValue::try_from_array(a, i).ok()?;
        let bv = ScalarValue::try_from_array(b, i).ok()?;
        match cypher_compare(&av, &bv)? {
            0 => {}
            c => return Some(c),
        }
    }
    Some(a.len().cmp(&b.len()) as i8) // a shared prefix → the shorter list sorts first
}

/// Cypher ORDERABILITY total order, used by `min`/`max` (which exclude nulls).
/// Ascending type rank `list < string < boolean < number < map`, then by value
/// within a type (numbers by `f64`, strings/booleans naturally, lists
/// lexicographically, maps by sorted `(key, value)` entries — ADR 0011 slice 5).
/// Distinct from [`cypher_compare`]: orderability is a TOTAL order across types,
/// whereas comparability (`<`) is three-valued with no cross-type order.
pub(super) fn cypher_order(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a = unwrap_het(a.clone());
    let b = unwrap_het(b.clone());
    let rank = |v: &ScalarValue| -> u8 {
        match v {
            ScalarValue::List(_) | ScalarValue::LargeList(_) => 1,
            ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) => 2,
            ScalarValue::Boolean(_) => 3,
            _ if is_numeric_scalar(v) => 4,
            ScalarValue::Struct(_) => 5,
            _ => 0,
        }
    };
    let (ra, rb) = (rank(&a), rank(&b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (&a, &b) {
        (ScalarValue::List(x), ScalarValue::List(y)) => cypher_seq_order(&x.value(0), &y.value(0)),
        (ScalarValue::Utf8(Some(x)), ScalarValue::Utf8(Some(y))) => x.cmp(y),
        (ScalarValue::Boolean(Some(x)), ScalarValue::Boolean(Some(y))) => x.cmp(y),
        (ScalarValue::Struct(x), ScalarValue::Struct(y)) => cypher_map_order(x, y),
        _ => match (scalar_as_f64(&a), scalar_as_f64(&b)) {
            (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        },
    }
}

/// Orderability of two maps (ADR 0011 slice 5): compare their entries sorted by
/// key — key-by-key (lexicographic), then value-by-value ([`cypher_order`]); a
/// map with fewer keys sorts first when it is a prefix of the other.
fn cypher_map_order(
    a: &datafusion::arrow::array::StructArray,
    b: &datafusion::arrow::array::StructArray,
) -> std::cmp::Ordering {
    let sorted = |s: &datafusion::arrow::array::StructArray| -> Vec<(String, ScalarValue)> {
        let mut e: Vec<(String, ScalarValue)> = s
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                (
                    f.name().clone(),
                    ScalarValue::try_from_array(s.column(i), 0).unwrap_or(ScalarValue::Null),
                )
            })
            .collect();
        e.sort_by(|x, y| x.0.cmp(&y.0));
        e
    };
    let (ea, eb) = (sorted(a), sorted(b));
    for ((ka, va), (kb, vb)) in ea.iter().zip(eb.iter()) {
        match ka.cmp(kb) {
            std::cmp::Ordering::Equal => {}
            c => return c,
        }
        match cypher_order(va, vb) {
            std::cmp::Ordering::Equal => {}
            c => return c,
        }
    }
    ea.len().cmp(&eb.len())
}

/// Lexicographic orderability of two list element arrays (shorter prefix sorts
/// first), decoding tagged elements.
fn cypher_seq_order(
    a: &datafusion::arrow::array::ArrayRef,
    b: &datafusion::arrow::array::ArrayRef,
) -> std::cmp::Ordering {
    let common = a.len().min(b.len());
    for i in 0..common {
        let av = ScalarValue::try_from_array(a, i).unwrap_or(ScalarValue::Null);
        let bv = ScalarValue::try_from_array(b, i).unwrap_or(ScalarValue::Null);
        match cypher_order(&av, &bv) {
            std::cmp::Ordering::Equal => {}
            c => return c,
        }
    }
    a.len().cmp(&b.len())
}

/// Three-valued Cypher equality of two scalar values: `Some(true)`,
/// `Some(false)`, or `None` (= `null`). Numbers compare across `Int`/`Float`;
/// lists and maps compare structurally with the Cypher rules — a length or
/// key-set mismatch is `false`, a `null` element with no definitive inequality
/// is `null`; otherwise different types are `false`. (ADR 0009)
pub(super) fn cypher_value_eq(l: &ScalarValue, r: &ScalarValue) -> Option<bool> {
    if l.is_null() || r.is_null() {
        return None;
    }
    // A heterogeneous-list element (ADR 0011 tagged struct) decodes to its plain
    // value, then compares by normal Cypher rules — so a tagged `int = 1` equals a
    // native `1`, and a native list equals a tagged list element-by-element.
    if let Some(dl) = decode_het(l) {
        return cypher_value_eq(&dl, r);
    }
    if let Some(dr) = decode_het(r) {
        return cypher_value_eq(l, &dr);
    }
    match (l, r) {
        (ScalarValue::List(a), ScalarValue::List(b)) => cypher_seq_eq(&a.value(0), &b.value(0)),
        (ScalarValue::LargeList(a), ScalarValue::LargeList(b)) => {
            cypher_seq_eq(&a.value(0), &b.value(0))
        }
        // Temporal values compare by their components, never as maps: an
        // absent optional component (a datetime's named zone) is a value, not
        // an unknown (#1887 D11).
        (ScalarValue::Struct(_), ScalarValue::Struct(_))
            if temporal_kind(l).is_some() || temporal_kind(r).is_some() =>
        {
            temporal_value_eq(l, r)
        }
        // Node/relationship/path values compare by **identity** (#1887 D1/D12).
        // Plain maps use three-valued structural equality (a null value
        // propagates: `{a: null} = {a: null}` is `null`, not `true`).
        (ScalarValue::Struct(a), ScalarValue::Struct(b)) => {
            if is_entity_struct(a) || is_entity_struct(b) {
                entity_identity_eq(a, b)
            } else {
                cypher_struct_eq(a, b)
            }
        }
        // Where a plan carries a node or relationship in scalar form it is its
        // 16-byte UUID; compare that against a whole value's identity.
        (ScalarValue::Struct(entity), ScalarValue::FixedSizeBinary(16, Some(_)))
        | (ScalarValue::FixedSizeBinary(16, Some(_)), ScalarValue::Struct(entity))
            if matches!(
                entity_kind(entity),
                Some(EntityKind::Node | EntityKind::Relationship)
            ) =>
        {
            let uuid = if matches!(l, ScalarValue::FixedSizeBinary(..)) {
                l
            } else {
                r
            };
            let name = if entity_kind(entity) == Some(EntityKind::Node) {
                "node_uuid"
            } else {
                "edge_uuid"
            };
            let identity = entity
                .column_by_name(name)
                .and_then(|column| ScalarValue::try_from_array(column, 0).ok())?;
            cypher_value_eq(&identity, uuid)
        }
        _ if is_numeric_scalar(l) && is_numeric_scalar(r) => {
            if let (Some(li), Some(ri)) = (scalar_as_i128(l), scalar_as_i128(r)) {
                return Some(li == ri);
            }
            let (Some(lf), Some(rf)) = (scalar_as_f64(l), scalar_as_f64(r)) else {
                return None;
            };
            Some(!lf.is_nan() && !rf.is_nan() && lf == rf)
        }
        // Same-typed scalar ⇒ direct equality; different types ⇒ false.
        _ if std::mem::discriminant(l) == std::mem::discriminant(r) => Some(l == r),
        _ => Some(false),
    }
}

/// A hash of `v` consistent with [`cypher_value_eq`]: whenever
/// `cypher_value_eq(a, b) == Some(true)`, `cypher_equality_hash(a)` and
/// `cypher_equality_hash(b)` are the same `Some` value. `None` means `v` is never
/// `Some(true)`-equal to anything (a NaN, a `null`, or a list or map holding one),
/// so it need not be looked up.
///
/// The converse does not hold and cannot: equality is not an equivalence
/// relation (NaN is unequal to itself; `Int`/`Float` equality goes through
/// `f64`, so it is not transitive above 2^53), so the hash only buckets values
/// and equality is still decided by `cypher_value_eq`.
pub(super) fn cypher_equality_hash(v: &ScalarValue) -> Option<u64> {
    let mut hasher = std::hash::DefaultHasher::new();
    feed_equality_hash(v, &mut hasher).then(|| hasher.finish())
}

/// Feed the equality-relevant content of `v` to `h`, mirroring the arms of
/// [`cypher_value_eq`]. `false` means the value is never equal to anything.
fn feed_equality_hash(v: &ScalarValue, h: &mut std::hash::DefaultHasher) -> bool {
    if v.is_null() {
        return false;
    }
    if let Some(decoded) = decode_het(v) {
        return feed_equality_hash(&decoded, h);
    }
    match v {
        ScalarValue::List(list) => feed_seq_hash(0, &list.value(0), h),
        ScalarValue::LargeList(list) => feed_seq_hash(1, &list.value(0), h),
        ScalarValue::Struct(s) => {
            if let Some(kind) = temporal_kind(v) {
                feed_temporal_hash(kind, s, h)
            } else if let Some(kind) = entity_kind(s) {
                feed_entity_hash(kind, s, h)
            } else {
                feed_map_hash(s, h)
            }
        }
        _ if is_numeric_scalar(v) => {
            // `1` equals `1.0`, and `0.0` equals `-0.0`: key every number by the
            // canonical bits of the `f64` that `cypher_value_eq` compares.
            let Some(f) = scalar_as_f64(v) else {
                return false;
            };
            if f.is_nan() {
                return false;
            }
            let canonical = if f == 0.0 { 0.0_f64 } else { f };
            2_u8.hash(h);
            canonical.to_bits().hash(h);
            true
        }
        // Same-typed scalars compare with `ScalarValue`'s own equality, which its
        // `Hash` agrees with for these flat variants. Anything else (a fixed-size
        // list, a map, a dictionary, ...) is bucketed by type alone: coarse but
        // correct.
        _ => {
            3_u8.hash(h);
            std::mem::discriminant(v).hash(h);
            if matches!(
                v,
                ScalarValue::Boolean(_)
                    | ScalarValue::Decimal32(..)
                    | ScalarValue::Decimal64(..)
                    | ScalarValue::Decimal128(..)
                    | ScalarValue::Decimal256(..)
                    | ScalarValue::Utf8(_)
                    | ScalarValue::LargeUtf8(_)
                    | ScalarValue::Utf8View(_)
                    | ScalarValue::Binary(_)
                    | ScalarValue::LargeBinary(_)
                    | ScalarValue::BinaryView(_)
                    | ScalarValue::FixedSizeBinary(..)
                    | ScalarValue::Date32(_)
                    | ScalarValue::Date64(_)
                    | ScalarValue::Time32Second(_)
                    | ScalarValue::Time32Millisecond(_)
                    | ScalarValue::Time64Microsecond(_)
                    | ScalarValue::Time64Nanosecond(_)
                    | ScalarValue::TimestampSecond(..)
                    | ScalarValue::TimestampMillisecond(..)
                    | ScalarValue::TimestampMicrosecond(..)
                    | ScalarValue::TimestampNanosecond(..)
                    | ScalarValue::DurationSecond(_)
                    | ScalarValue::DurationMillisecond(_)
                    | ScalarValue::DurationMicrosecond(_)
                    | ScalarValue::DurationNanosecond(_)
                    | ScalarValue::IntervalYearMonth(_)
                    | ScalarValue::IntervalDayTime(_)
                    | ScalarValue::IntervalMonthDayNano(_)
            ) {
                v.hash(h);
            }
            true
        }
    }
}

/// Lists are equal only when element-wise equal with no `null` in play, so one
/// unhashable element makes the whole list unhashable.
fn feed_seq_hash(
    tag: u8,
    elements: &datafusion::arrow::array::ArrayRef,
    h: &mut std::hash::DefaultHasher,
) -> bool {
    tag.hash(h);
    elements.len().hash(h);
    (0..elements.len()).all(|i| {
        ScalarValue::try_from_array(elements, i)
            .is_ok_and(|element| feed_equality_hash(&element, h))
    })
}

fn feed_temporal_hash(
    kind: TemporalKind,
    s: &datafusion::arrow::array::StructArray,
    h: &mut std::hash::DefaultHasher,
) -> bool {
    4_u8.hash(h);
    match kind {
        TemporalKind::Date => date_struct_value(s, 0).map(|p| (0_u8, p).hash(h)),
        TemporalKind::LocalDateTime => localdatetime_struct_parts(s, 0).map(|p| (1_u8, p).hash(h)),
        TemporalKind::Time => time_struct_parts(s, 0).map(|p| (2_u8, p).hash(h)),
        TemporalKind::DateTime => datetime_struct_parts(s, 0).map(|p| (3_u8, p).hash(h)),
        TemporalKind::Duration => duration_struct_parts(s, 0)
            .map(|d| (4_u8, d.months, d.days, d.seconds, d.nanos).hash(h)),
    }
    .is_some()
}

/// A node or relationship hashes as its identity scalar, because it also equals
/// that bare 16-byte UUID; a path hashes as its node and relationship sequences.
fn feed_entity_hash(
    kind: EntityKind,
    s: &datafusion::arrow::array::StructArray,
    h: &mut std::hash::DefaultHasher,
) -> bool {
    let mut field = |name: &str| {
        s.column_by_name(name)
            .and_then(|column| ScalarValue::try_from_array(column, 0).ok())
            .is_some_and(|value| feed_equality_hash(&value, h))
    };
    match kind {
        EntityKind::Node => field("node_uuid"),
        EntityKind::Relationship => field("edge_uuid"),
        EntityKind::Path => field("nodes") && field("relationships"),
    }
}

/// Maps are equal when their key sets and values are, whatever the field order.
fn feed_map_hash(
    s: &datafusion::arrow::array::StructArray,
    h: &mut std::hash::DefaultHasher,
) -> bool {
    let mut keys: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
    keys.sort_unstable();
    5_u8.hash(h);
    keys.hash(h);
    keys.iter().all(|key| {
        s.column_by_name(key)
            .and_then(|column| ScalarValue::try_from_array(column, 0).ok())
            .is_some_and(|value| feed_equality_hash(&value, h))
    })
}

/// First-seen-ordered membership index for `collect(DISTINCT)`: buckets the
/// admitted values by [`cypher_equality_hash`] so a probe compares against the
/// few values sharing its hash instead of every value seen.
#[derive(Debug, Default)]
pub(super) struct DistinctIndex {
    /// Hash to the most recently admitted value with that hash.
    heads: std::collections::HashMap<u64, usize>,
    /// Per admitted value, the previous value in its bucket (`NO_LINK` ends the
    /// chain, and marks an unhashable value, which belongs to no bucket).
    links: Vec<usize>,
}

const NO_LINK: usize = usize::MAX;

impl DistinctIndex {
    /// Append `v` to `values` unless a value already there is `Some(true)`-equal
    /// to it under `eq`; returns whether it was appended. `values` must only be
    /// appended to through this index. `eq` must be [`cypher_value_eq`] (the
    /// parameter exists so a test can count comparisons).
    pub(super) fn admit(
        &mut self,
        values: &mut Vec<ScalarValue>,
        v: ScalarValue,
        eq: impl Fn(&ScalarValue, &ScalarValue) -> Option<bool>,
    ) -> bool {
        debug_assert_eq!(values.len(), self.links.len());
        let Some(hash) = cypher_equality_hash(&v) else {
            self.links.push(NO_LINK);
            values.push(v);
            return true;
        };
        let head = self.heads.get(&hash).copied();
        let mut cursor = head;
        while let Some(index) = cursor {
            if eq(&values[index], &v) == Some(true) {
                return false;
            }
            cursor = Some(self.links[index]).filter(|&link| link != NO_LINK);
        }
        self.heads.insert(hash, values.len());
        self.links.push(head.unwrap_or(NO_LINK));
        values.push(v);
        true
    }

    /// Heap bytes held by the index (the struct itself is counted by its owner).
    pub(super) fn heap_size(&self) -> usize {
        self.heads.capacity() * (std::mem::size_of::<u64>() + std::mem::size_of::<usize>() + 1)
            + self.links.capacity() * std::mem::size_of::<usize>()
    }
}

/// Decode a heterogeneous-list element (ADR 0011 tagged struct) to its plain
/// `ScalarValue`; `None` for any non-tagged value (so normal values pass through
/// `cypher_value_eq` unchanged). A null element → `Null`; a list element
/// (`__het_tag == 4`) → a `List` of tagged children; a map element
/// (`__het_tag == 5`) → a `Struct` map whose values stay tagged (decoded in turn
/// by recursion through `cypher_value_eq`).
// UDF admission keeps the pure three-valued comparison helpers operating on
// checked values. Malformed wire data is an error, never Cypher null.
pub(super) fn validate_heterogeneous_arguments(
    values: &[ColumnarValue],
) -> datafusion::error::Result<()> {
    for value in values {
        let data_type = match value {
            ColumnarValue::Array(array) => array.data_type().clone(),
            ColumnarValue::Scalar(scalar) => scalar.data_type(),
        };
        if graphforge_value::heterogeneous::contains_heterogeneous(&data_type)
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?
        {
            let array = value.to_array(1)?;
            graphforge_value::heterogeneous::validate_array(array.as_ref())
                .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
        }
    }
    Ok(())
}

pub(super) fn decode_het(s: &ScalarValue) -> Option<ScalarValue> {
    decode_het_scalar(s).ok().flatten()
}

/// Decode one tagged value, distinguishing ordinary values from malformed wire data.
///
/// # Errors
/// Returns the shared typed schema/tag error for malformed heterogeneous values.
pub fn decode_het_scalar(value: &ScalarValue) -> datafusion::error::Result<Option<ScalarValue>> {
    use datafusion::arrow::array::{ArrayRef, ListArray, StringArray, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};
    use graphforge_value::heterogeneous::{self as het, Decoded};
    let ScalarValue::Struct(array) = value else {
        return Ok(None);
    };
    let error = |error| datafusion::error::DataFusionError::External(Box::new(error));
    if het::recognize(array.data_type()).map_err(error)?.is_none() {
        return Ok(None);
    }
    let decoded = match het::decode_row(array, 0).map_err(error)? {
        Decoded::Null => ScalarValue::Null,
        Decoded::Payload(payload) => ScalarValue::try_from_array(payload, 0)?,
        Decoded::Map(payload) => {
            let entries = payload
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| error(het::ValueError::Schema))?
                .value(0);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| error(het::ValueError::Schema))?;
            let keys = entries
                .column_by_name(het::MAP_KEY)
                .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| error(het::ValueError::Schema))?;
            let values = entries
                .column_by_name(het::MAP_VALUE)
                .ok_or_else(|| error(het::ValueError::Schema))?;
            let mut fields = Vec::with_capacity(entries.len());
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(entries.len());
            for row in 0..entries.len() {
                if keys.is_null(row) {
                    return Err(error(het::ValueError::NullPayload));
                }
                let value = values.slice(row, 1);
                fields.push(Field::new(keys.value(row), value.data_type().clone(), true));
                columns.push(value);
            }
            if entries.is_empty() {
                ScalarValue::Struct(Arc::new(StructArray::new_empty_fields(1, None)))
            } else {
                ScalarValue::Struct(Arc::new(StructArray::try_new(
                    Fields::from(fields),
                    columns,
                    None,
                )?))
            }
        }
    };
    Ok(Some(decoded))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TemporalKind {
    Date,
    LocalDateTime,
    Time,
    DateTime,
    Duration,
}

fn temporal_kind(value: &ScalarValue) -> Option<TemporalKind> {
    let data_type = value.data_type();
    if is_date_struct(&data_type) {
        Some(TemporalKind::Date)
    } else if is_localdatetime_struct(&data_type) {
        Some(TemporalKind::LocalDateTime)
    } else if is_time_struct(&data_type) {
        Some(TemporalKind::Time)
    } else if is_datetime_struct(&data_type) {
        Some(TemporalKind::DateTime)
    } else if is_duration_struct(&data_type) {
        Some(TemporalKind::Duration)
    } else {
        None
    }
}

/// Equality of two non-null struct values at least one of which is temporal.
/// Values of different kinds are unequal. Values of one kind are equal when
/// every component is: a `time` and a `datetime` compare their offset (and a
/// `datetime` its named zone) as well as their instant, so two values that
/// order equal but render differently are unequal.
fn temporal_value_eq(l: &ScalarValue, r: &ScalarValue) -> Option<bool> {
    let (ScalarValue::Struct(a), ScalarValue::Struct(b)) = (l, r) else {
        return Some(false);
    };
    let kind = temporal_kind(l);
    if kind != temporal_kind(r) {
        return Some(false);
    }
    match kind? {
        TemporalKind::Date => Some(date_struct_value(a, 0)? == date_struct_value(b, 0)?),
        TemporalKind::LocalDateTime => {
            Some(localdatetime_struct_parts(a, 0)? == localdatetime_struct_parts(b, 0)?)
        }
        TemporalKind::Time => Some(time_struct_parts(a, 0)? == time_struct_parts(b, 0)?),
        TemporalKind::DateTime => {
            Some(datetime_struct_parts(a, 0)? == datetime_struct_parts(b, 0)?)
        }
        TemporalKind::Duration => {
            let parts =
                |s| duration_struct_parts(s, 0).map(|d| (d.months, d.days, d.seconds, d.nanos));
            Some(parts(a)? == parts(b)?)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntityKind {
    Node,
    Relationship,
    Path,
}

fn entity_kind(s: &datafusion::arrow::array::StructArray) -> Option<EntityKind> {
    if is_path_struct(s) {
        Some(EntityKind::Path)
    } else if is_rel_struct(s) {
        Some(EntityKind::Relationship)
    } else if is_node_struct(s) {
        Some(EntityKind::Node)
    } else {
        None
    }
}

/// Identity equality of graph values (#1887 D1/D12): two nodes are equal when
/// their `node_uuid`s are, two relationships when their `edge_uuid`s are, and
/// two paths when their node and relationship sequences are element-wise
/// equal. Labels and properties are not identity: the same node read through
/// differently shaped projections (a bare variable, a list element, a
/// collected value) carries different property columns. A graph value never
/// equals a value of another kind.
fn entity_identity_eq(
    a: &datafusion::arrow::array::StructArray,
    b: &datafusion::arrow::array::StructArray,
) -> Option<bool> {
    let kind = entity_kind(a);
    if kind != entity_kind(b) {
        return Some(false);
    }
    let field = |s: &datafusion::arrow::array::StructArray, name: &str| {
        s.column_by_name(name)
            .and_then(|column| ScalarValue::try_from_array(column, 0).ok())
    };
    let identity = |name: &str| -> Option<bool> {
        let (Some(left), Some(right)) = (field(a, name), field(b, name)) else {
            return Some(false);
        };
        cypher_value_eq(&left, &right)
    };
    match kind? {
        EntityKind::Node => identity("node_uuid"),
        EntityKind::Relationship => identity("edge_uuid"),
        EntityKind::Path => {
            if identity("nodes")? {
                identity("relationships")
            } else {
                Some(false)
            }
        }
    }
}

/// Whether a `Struct` is a node / relationship / path value (whose equality is
/// identity-based) rather than a Cypher map (three-valued structural equality),
/// detected by the reserved field names those values carry.
fn is_entity_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.fields().iter().any(|f| {
        matches!(
            f.name().as_str(),
            "node_uuid" | "src_uuid" | "dst_uuid" | "nodes" | "relationships" | "labels"
        )
    })
}

// ---------------------------------------------------------------------------
// cypher_date_component UDF
// ---------------------------------------------------------------------------
/// Integer scalar as `i128` for exact cross-width integer equality/comparison.
pub(super) fn scalar_as_i128(v: &ScalarValue) -> Option<i128> {
    match v {
        ScalarValue::Int8(Some(n)) => Some(i128::from(*n)),
        ScalarValue::Int16(Some(n)) => Some(i128::from(*n)),
        ScalarValue::Int32(Some(n)) => Some(i128::from(*n)),
        ScalarValue::Int64(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt8(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt16(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt32(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt64(Some(n)) => Some(i128::from(*n)),
        _ => None,
    }
}

/// Numeric scalar as `f64` for cross integer/float equality and ordering.
pub(super) fn scalar_as_f64(v: &ScalarValue) -> Option<f64> {
    #[allow(
        clippy::cast_precision_loss,
        reason = "only used for mixed integer/float numeric semantics; pure integers use i128"
    )]
    match v {
        ScalarValue::Int8(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Int16(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Int32(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Int64(Some(n)) => Some(*n as f64),
        ScalarValue::UInt8(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt16(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt32(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt64(Some(n)) => Some(*n as f64),
        ScalarValue::Float32(Some(f)) => Some(f64::from(*f)),
        ScalarValue::Float64(Some(f)) => Some(*f),
        _ => None,
    }
}

/// Three-valued Cypher equality of two list element arrays.
fn cypher_seq_eq(
    a: &datafusion::arrow::array::ArrayRef,
    b: &datafusion::arrow::array::ArrayRef,
) -> Option<bool> {
    if a.len() != b.len() {
        return Some(false); // length mismatch ⇒ false, even with nulls present
    }
    let mut saw_null = false;
    for i in 0..a.len() {
        let av = ScalarValue::try_from_array(a, i).ok()?;
        let bv = ScalarValue::try_from_array(b, i).ok()?;
        match cypher_value_eq(&av, &bv) {
            Some(false) => return Some(false),
            None => saw_null = true,
            Some(true) => {}
        }
    }
    if saw_null { None } else { Some(true) }
}

/// Three-valued Cypher equality of two map (`Struct`) values.
fn cypher_struct_eq(
    a: &datafusion::arrow::array::StructArray,
    b: &datafusion::arrow::array::StructArray,
) -> Option<bool> {
    // Key sets must match — a key with a `null` value still counts as present.
    let mut ka: Vec<&str> = a.fields().iter().map(|f| f.name().as_str()).collect();
    let mut kb: Vec<&str> = b.fields().iter().map(|f| f.name().as_str()).collect();
    ka.sort_unstable();
    kb.sort_unstable();
    if ka != kb {
        return Some(false);
    }
    let mut saw_null = false;
    for key in ka {
        let av = ScalarValue::try_from_array(a.column_by_name(key)?, 0).ok()?;
        let bv = ScalarValue::try_from_array(b.column_by_name(key)?, 0).ok()?;
        match cypher_value_eq(&av, &bv) {
            Some(false) => return Some(false),
            None => saw_null = true,
            Some(true) => {}
        }
    }
    if saw_null { None } else { Some(true) }
}

#[cfg(test)]
mod tests {
    use super::super::tests::invoke_test_udf;
    use super::super::{build_het_struct, datetime_scalar, het_depth, time_scalar};
    use super::*;

    #[test]
    fn comparison_error_contract_matrix_is_exact() {
        assert_eq!(scalar_as_i8(&ScalarValue::Int8(Some(-7))).unwrap(), -7);
        assert_eq!(scalar_as_i8(&ScalarValue::Int64(Some(7))).unwrap(), 7);
        assert!(
            scalar_as_i8(&ScalarValue::Int64(Some(128)))
                .unwrap_err()
                .to_string()
                .contains("outside i8 range")
        );
        assert_eq!(
            scalar_as_i8(&ScalarValue::Utf8(Some("1".into())))
                .unwrap_err()
                .to_string(),
            "Error during planning: comparison opcode must be an integer, got Utf8(\"1\")"
        );
    }

    #[test]
    fn cypher_value_eq_scalars() {
        use datafusion::scalar::ScalarValue as S;
        let i = |n| S::Int64(Some(n));

        assert_eq!(cypher_value_eq(&i(1), &i(1)), Some(true));
        assert_eq!(cypher_value_eq(&i(1), &i(2)), Some(false));
        // number vs string ⇒ false (different types are never equal, not an error)
        assert_eq!(
            cypher_value_eq(&i(1), &S::Utf8(Some("1".into()))),
            Some(false)
        );
        // 1 = 1.0 (cross numeric)
        assert_eq!(cypher_value_eq(&i(1), &S::Float64(Some(1.0))), Some(true));
        assert_eq!(cypher_value_eq(&i(1), &S::UInt64(Some(1))), Some(true));
        assert_eq!(
            cypher_value_eq(&S::Float64(Some(f64::NAN)), &S::Float64(Some(f64::NAN))),
            Some(false)
        );
        // null propagates
        assert_eq!(cypher_value_eq(&i(1), &S::Int64(None)), None);
        assert_eq!(cypher_value_eq(&S::Null, &i(1)), None);
    }

    #[test]
    fn cypher_comparison_predicate_handles_nan_and_cross_type() {
        use datafusion::scalar::ScalarValue as S;

        assert_eq!(
            cypher_compare_pred(&S::Float64(Some(f64::NAN)), &S::Int64(Some(1)), 2),
            Some(false)
        );
        assert_eq!(
            cypher_compare_pred(&S::Utf8(Some("1".to_owned())), &S::Int64(Some(1)), 0,),
            None
        );
        assert_eq!(
            cypher_compare_pred(&S::Int64(Some(1)), &S::Float64(Some(2.0)), 0),
            Some(true)
        );
        assert_eq!(
            cypher_compare_pred(&S::UInt64(Some(1)), &S::Int64(Some(2)), 0),
            Some(true)
        );
    }

    #[test]
    fn cypher_xor_implements_three_valued_truth_table() {
        use datafusion::arrow::array::{Array, BooleanArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let left = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            None,
            None,
            None,
        ]);
        let right = BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
        ]);
        let expected = [
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
            None,
            None,
            None,
            None,
        ];
        let field = Arc::new(Field::new("value", DataType::Boolean, true));
        let arguments = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            arg_fields: vec![Arc::clone(&field), field],
            number_rows: expected.len(),
            return_field: Arc::new(Field::new("xor", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CypherBoolOp::new(CypherBoolOpKind::Xor)
            .invoke_with_args(arguments)
            .expect("XOR evaluates");
        let ColumnarValue::Array(result) = result else {
            panic!("array inputs must produce an array")
        };
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("XOR result is boolean");

        for (index, expected) in expected.into_iter().enumerate() {
            let actual = (!result.is_null(index)).then(|| result.value(index));
            assert_eq!(actual, expected, "truth-table row {index}");
        }
    }

    #[test]
    fn zoned_temporal_order_keys_compare_absolute_instants() {
        let hour = 3_600_000_000_000_i64;
        let early_time = time_scalar(Some((12 * hour + 35 * 60_000_000_000, 5 * 3_600)));
        let late_time = time_scalar(Some((10 * hour + 35 * 60_000_000_000, -8 * 3_600)));
        assert!(cypher_order_key(&early_time) < cypher_order_key(&late_time));

        let earlier_datetime = datetime_scalar(Some((5_000, 12 * hour, 3_600, None)));
        let later_datetime = datetime_scalar(Some((5_000, 12 * hour, 0, None)));
        assert!(cypher_order_key(&earlier_datetime) < cypher_order_key(&later_datetime));
    }

    #[test]
    fn zoned_temporal_structs_require_cypher_order_keys() {
        assert!(needs_cypher_order_key_type(&time_scalar(None).data_type()));
        assert!(needs_cypher_order_key_type(
            &datetime_scalar(None).data_type()
        ));
    }

    #[test]
    fn cypher_value_comparison_and_order_helpers_cover_cross_type_edges() {
        use datafusion::scalar::ScalarValue as S;

        for value in [
            S::Int8(Some(1)),
            S::Int16(Some(1)),
            S::Int32(Some(1)),
            S::Int64(Some(1)),
            S::UInt8(Some(1)),
            S::UInt16(Some(1)),
            S::UInt32(Some(1)),
            S::UInt64(Some(1)),
            S::Float32(Some(1.0)),
            S::Float64(Some(1.0)),
        ] {
            assert_eq!(scalar_as_f64(&value), Some(1.0));
        }
        assert_eq!(scalar_as_i128(&S::Float64(Some(1.0))), None);
        assert_eq!(scalar_as_f64(&S::Boolean(Some(true))), None);

        let one = S::List(S::new_list(&[S::Int64(Some(1))], &DataType::Int64, true));
        let one_null = S::List(S::new_list(
            &[S::Int64(Some(1)), S::Int64(None)],
            &DataType::Int64,
            true,
        ));
        let two = S::List(S::new_list(
            &[S::Int64(Some(1)), S::Int64(Some(2))],
            &DataType::Int64,
            true,
        ));
        assert_eq!(cypher_value_eq(&one, &one), Some(true));
        assert_eq!(cypher_value_eq(&one, &two), Some(false));
        assert_eq!(cypher_value_eq(&one_null, &one_null), None);
        assert_eq!(cypher_value_eq(&S::Null, &S::Int64(Some(1))), None);
        assert_eq!(
            cypher_value_eq(&S::Int64(Some(1)), &S::Float64(Some(1.0))),
            Some(true)
        );
        assert_eq!(
            cypher_value_eq(&S::Utf8(Some("a".into())), &S::Utf8(Some("b".into()))),
            Some(false)
        );

        assert!(cypher_order_key(&S::Null).starts_with("99:null"));
        assert!(cypher_order_key(&S::Utf8(Some("a".into()))).starts_with("60:str"));
        assert!(cypher_order_key(&S::Boolean(Some(true))).starts_with("70:bool"));
        assert!(cypher_order_key(&S::Float64(Some(f64::NAN))).starts_with("90:nan"));
        assert!(cypher_order_key(&S::Binary(Some(vec![1]))).starts_with("98:other"));
        assert!(cypher_order_key(&one).starts_with("40:list"));
    }

    #[test]
    fn exact_zero_total_order_keys_distinguish_core_cypher_value_domains() {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
            &DataType::Int64,
            true,
        ));
        let values = [
            ScalarValue::Null,
            list,
            ScalarValue::Utf8(Some("text".into())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Int64(Some(-2)),
            ScalarValue::Float64(Some(2.5)),
            ScalarValue::Float64(Some(f64::NAN)),
        ];
        let keys = values.iter().map(cypher_order_key).collect::<Vec<_>>();
        assert!(keys[0].starts_with("99:null"));
        assert!(keys[1].starts_with("40:list"));
        assert!(keys[2].starts_with("60:str"));
        assert!(keys[3].starts_with("70:bool"));
        assert!(keys[4].starts_with("80:num"));
        assert!(keys[5].starts_with("80:num"));
        assert!(keys[6].starts_with("90:nan"));
        assert_eq!(
            cypher_order(
                &ScalarValue::Utf8(Some("a".into())),
                &ScalarValue::Boolean(Some(false))
            ),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn boolean_udf_runtime_truth_tables() {
        use datafusion::arrow::array::{Array, BooleanArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        fn invoke<U: ScalarUDFImpl>(
            udf: &U,
            values: Vec<ScalarValue>,
            return_type: DataType,
        ) -> datafusion::error::Result<ColumnarValue> {
            let fields = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    Arc::new(Field::new(format!("arg_{index}"), value.data_type(), true))
                })
                .collect();
            udf.invoke_with_args(ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Scalar).collect(),
                arg_fields: fields,
                number_rows: 1,
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
        }

        let booleans = [None, Some(false), Some(true)];
        for kind in [
            CypherBoolOpKind::And,
            CypherBoolOpKind::Or,
            CypherBoolOpKind::Xor,
        ] {
            for left in booleans {
                for right in booleans {
                    let output = invoke(
                        &CypherBoolOp::new(kind),
                        vec![ScalarValue::Boolean(left), ScalarValue::Boolean(right)],
                        DataType::Boolean,
                    )
                    .unwrap();
                    let output = match output {
                        ColumnarValue::Array(array) => array,
                        ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
                    };
                    let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let actual = (!output.is_null(0)).then(|| output.value(0));
                    let expected = match kind {
                        CypherBoolOpKind::And => match (left, right) {
                            (Some(false), _) | (_, Some(false)) => Some(false),
                            (Some(true), Some(true)) => Some(true),
                            _ => None,
                        },
                        CypherBoolOpKind::Or => match (left, right) {
                            (Some(true), _) | (_, Some(true)) => Some(true),
                            (Some(false), Some(false)) => Some(false),
                            _ => None,
                        },
                        CypherBoolOpKind::Xor => left.zip(right).map(|(l, r)| l ^ r),
                    };
                    assert_eq!(actual, expected);
                }
            }
        }
        assert!(
            invoke(
                &CypherBoolOp::new(CypherBoolOpKind::And),
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Boolean(Some(true))
                ],
                DataType::Boolean,
            )
            .unwrap_err()
            .to_string()
            .contains("expected boolean operand")
        );
    }

    #[test]
    fn sequence_order_preserves_length_and_value_ordering() {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        let shorter: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let longer: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let different: ArrayRef = Arc::new(Int64Array::from(vec![1, 9]));
        assert_eq!(
            cypher_seq_order(&shorter, &longer),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            cypher_seq_order(&different, &shorter),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            cypher_seq_order(&shorter, &shorter),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn malformed_heterogeneous_values_fail_ordering_admission() {
        use graphforge_value::heterogeneous::{self as het, Scalar};
        let original = het::encode_scalar([Some(Scalar::Int(7))]);
        let mut columns = original.columns().to_vec();
        columns[0] = Arc::new(datafusion::arrow::array::Int8Array::from(vec![99]));
        let value = ScalarValue::Struct(Arc::new(datafusion::arrow::array::StructArray::new(
            het::scalar_fields(),
            columns,
            None,
        )));
        let error = invoke_test_udf(&CypherOrderKey::new(), vec![value.clone()]).unwrap_err();
        assert!(error.to_string().contains("GF_VALUE_TAG"), "{error}");
        let valid = ScalarValue::Struct(Arc::new(original));
        invoke_test_udf(&CypherOrderKey::new(), vec![valid]).unwrap();
    }

    fn invoke_cypher_in(
        lhs: datafusion::arrow::array::ArrayRef,
        rhs: datafusion::arrow::array::ArrayRef,
    ) -> datafusion::arrow::array::ArrayRef {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let n = lhs.len();
        let lhs_field = Arc::new(Field::new("lhs", lhs.data_type().clone(), true));
        let rhs_field = Arc::new(Field::new("rhs", rhs.data_type().clone(), true));
        let ret = Arc::new(Field::new("in", DataType::Boolean, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)],
            arg_fields: vec![lhs_field, rhs_field],
            number_rows: n,
            return_field: ret,
            config_options: Arc::new(ConfigOptions::default()),
        };
        match CypherIn::new().invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
        }
    }

    #[test]
    fn cypher_in_decodes_tagged_list_rhs() {
        use datafusion::arrow::array::{Array, BooleanArray};
        use datafusion::scalar::ScalarValue as S;
        use std::sync::Arc;

        let lhs = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]));
        let rhs_values = vec![
            S::List(S::new_list(
                &[S::Boolean(Some(true))],
                &DataType::Boolean,
                true,
            )),
            S::List(S::new_list(
                &[S::Boolean(Some(false))],
                &DataType::Boolean,
                true,
            )),
            S::List(S::new_list(&[S::Boolean(None)], &DataType::Boolean, true)),
            S::List(S::new_list(&[], &DataType::Boolean, true)),
        ];
        let depth = rhs_values.iter().filter_map(het_depth).max().unwrap();
        let rhs = Arc::new(build_het_struct(&rhs_values, depth).expect("tagged RHS"));
        let out = invoke_cypher_in(lhs, rhs);
        let bools = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(bools.value(0), "true IN [true]");
        assert!(!bools.value(1), "true IN [false]");
        assert!(bools.is_null(2), "true IN [null] -> null");
        assert!(!bools.value(3), "true IN []");
    }

    /// `cypher_equality_hash` and `DistinctIndex`: the bucketing behind
    /// `collect(DISTINCT)` must never separate two values `cypher_value_eq` calls
    /// equal, and must reproduce a full scan's output exactly.
    mod distinct_bucketing {
        use super::*;
        use crate::expr::tests::values::{
            float, int, large_list, list, map, node, path, relationship, text, uuid,
        };
        use crate::expr::{date_scalar, duration_scalar, localdatetime_scalar};
        use crate::temporal::DurationValue;
        use datafusion::arrow::datatypes::DataType;

        const HOUR: i64 = 3_600_000_000_000;

        fn ints(items: &[i64]) -> ScalarValue {
            crate::expr::tests::values::ints(items)
        }

        fn floats(items: &[f64]) -> ScalarValue {
            let items: Vec<_> = items.iter().copied().map(float).collect();
            list(&items, &DataType::Float64)
        }

        /// `value` as a heterogeneous-list element (the tagged encoding).
        fn tagged(value: &ScalarValue) -> ScalarValue {
            let depth = het_depth(value).expect("a het-encodable value");
            let array = build_het_struct(std::slice::from_ref(value), depth).expect("tagged");
            ScalarValue::try_from_array(&array, 0).expect("row 0")
        }

        fn duration(months: i64, days: i64, seconds: i64, nanos: i64) -> ScalarValue {
            duration_scalar(Some(DurationValue {
                months,
                days,
                seconds,
                nanos,
            }))
        }

        /// Values chosen to straddle every arm of `cypher_value_eq`: numerics that
        /// are equal across types, NaN and null in every position, containers in
        /// both list encodings and in tagged form, entities that share identity
        /// but not properties (and the reverse), and each temporal kind.
        fn corpus() -> Vec<ScalarValue> {
            let n = |id, x| node(id, x);
            vec![
                // numbers
                int(0),
                int(1),
                int(-1),
                int(2),
                int(1 << 53),
                int((1 << 53) + 1),
                int(i64::MAX),
                int(i64::MAX - 1),
                ScalarValue::Int32(Some(1)),
                ScalarValue::UInt64(Some(1)),
                ScalarValue::Float32(Some(1.0)),
                float(0.0),
                float(-0.0),
                float(1.0),
                float(1.5),
                float(2.0),
                float(9_223_372_036_854_775_808.0),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
                float(f64::NAN),
                // nulls
                ScalarValue::Int64(None),
                ScalarValue::Utf8(None),
                ScalarValue::Null,
                // strings and booleans
                text(""),
                text("a"),
                text("A"),
                text("1"),
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
                // lists, native and large, mixed element types, null and NaN inside
                ints(&[]),
                ints(&[1]),
                ints(&[1, 2]),
                ints(&[2, 1]),
                floats(&[1.0]),
                floats(&[1.0, 2.0]),
                floats(&[f64::NAN]),
                large_list(&[int(1)], &DataType::Int64),
                list(&[text("a")], &DataType::Utf8),
                list(&[ints(&[1])], &ints(&[1]).data_type()),
                list(&[floats(&[1.0])], &floats(&[1.0]).data_type()),
                list(&[int(1), ScalarValue::Int64(None)], &DataType::Int64),
                // maps: key order is irrelevant, null and NaN values are unhashable
                map(vec![("a", int(1)), ("b", int(2))]),
                map(vec![("b", int(2)), ("a", int(1))]),
                map(vec![("a", float(1.0)), ("b", int(2))]),
                map(vec![("a", int(1))]),
                map(vec![("a", ScalarValue::Int64(None))]),
                map(vec![("a", float(f64::NAN))]),
                map(vec![("m", map(vec![("a", int(1))]))]),
                map(vec![("m", map(vec![("a", float(1.0))]))]),
                // entities: identity decides, properties do not
                n(1, 5),
                n(1, 6),
                n(2, 5),
                relationship(1, 5),
                relationship(1, 6),
                relationship(2, 5),
                uuid(1),
                uuid(2),
                path(&[n(1, 0), n(2, 0)], &[relationship(7, 0)]),
                path(&[n(1, 9), n(2, 9)], &[relationship(7, 9)]),
                path(&[n(1, 0), n(3, 0)], &[relationship(7, 0)]),
                path(&[n(1, 0), n(2, 0)], &[relationship(8, 0)]),
                // temporals
                date_scalar(Some(0)),
                date_scalar(Some(1)),
                date_scalar(None),
                localdatetime_scalar(Some((0, 0))),
                localdatetime_scalar(Some((0, 1))),
                time_scalar(Some((12 * HOUR, 3_600))),
                time_scalar(Some((12 * HOUR, 0))),
                datetime_scalar(Some((5_000, 12 * HOUR, 3_600, None))),
                datetime_scalar(Some((5_000, 12 * HOUR, 3_600, Some("Europe/Paris".into())))),
                datetime_scalar(Some((5_000, 12 * HOUR, 0, None))),
                duration(0, 1, 0, 0),
                duration(0, 1, 0, 0),
                duration(1, 0, 0, 0),
                // tagged (heterogeneous-list element) forms
                tagged(&int(1)),
                tagged(&float(1.0)),
                tagged(&text("a")),
                tagged(&ints(&[1, 2])),
                tagged(&floats(&[1.0, 2.0])),
                tagged(&map(vec![("a", int(1)), ("b", int(2))])),
            ]
        }

        #[test]
        fn equal_values_share_a_hash_and_unhashable_values_equal_nothing() {
            let corpus = corpus();
            let mut equal_pairs = 0;
            let mut unhashable = 0;
            for (i, a) in corpus.iter().enumerate() {
                let ha = cypher_equality_hash(a);
                unhashable += usize::from(ha.is_none());
                for (j, b) in corpus.iter().enumerate() {
                    let hb = cypher_equality_hash(b);
                    if cypher_value_eq(a, b) == Some(true) {
                        equal_pairs += usize::from(i != j);
                        assert!(
                            ha.is_some() && ha == hb,
                            "{a:?} == {b:?} but their hashes are {ha:?} and {hb:?}"
                        );
                    }
                }
                if ha.is_none() {
                    assert!(
                        corpus.iter().all(|b| cypher_value_eq(a, b) != Some(true)
                            && cypher_value_eq(b, a) != Some(true)),
                        "{a:?} is unhashable but equals something"
                    );
                }
            }
            // The property must not be vacuous.
            assert!(
                equal_pairs >= 40,
                "only {equal_pairs} equal pairs in corpus"
            );
            assert!(
                unhashable >= 8,
                "only {unhashable} unhashable values in corpus"
            );
        }

        #[test]
        fn hash_agrees_across_numeric_types_zero_signs_and_encodings() {
            let h = |v: &ScalarValue| cypher_equality_hash(v).expect("hashable");
            assert_eq!(h(&int(1)), h(&float(1.0)));
            assert_eq!(h(&int(1)), h(&ScalarValue::UInt64(Some(1))));
            assert_eq!(h(&int(1)), h(&ScalarValue::Float32(Some(1.0))));
            assert_eq!(h(&float(0.0)), h(&float(-0.0)));
            assert_eq!(h(&int(0)), h(&float(-0.0)));
            assert_eq!(h(&ints(&[1, 2])), h(&floats(&[1.0, 2.0])));
            assert_eq!(h(&int(1)), h(&tagged(&int(1))));
            assert_eq!(h(&int(1)), h(&tagged(&float(1.0))));
            assert_eq!(h(&ints(&[1, 2])), h(&tagged(&floats(&[1.0, 2.0]))));
            assert_eq!(
                h(&map(vec![("a", int(1)), ("b", int(2))])),
                h(&map(vec![("b", int(2)), ("a", float(1.0))]))
            );
            // A node and a relationship are each equal to their bare identity.
            assert_eq!(h(&node(4, 0)), h(&node(4, 99)));
            assert_eq!(h(&node(4, 0)), h(&uuid(4)));
            assert_eq!(h(&relationship(4, 0)), h(&uuid(4)));
        }

        #[test]
        fn hash_separates_values_that_are_not_equal() {
            let h = |v: &ScalarValue| cypher_equality_hash(v).expect("hashable");
            let distinct = [
                int(1),
                int(2),
                float(1.5),
                text("a"),
                text("A"),
                text("1"),
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
                ints(&[1, 2]),
                ints(&[2, 1]),
                ints(&[1]),
                ints(&[]),
                large_list(&[int(1)], &DataType::Int64),
                map(vec![("a", int(1))]),
                map(vec![("b", int(1))]),
                node(1, 0),
                node(2, 0),
                path(&[node(1, 0)], &[]),
                date_scalar(Some(0)),
                localdatetime_scalar(Some((0, 0))),
                duration(0, 1, 0, 0),
                duration(1, 0, 0, 0),
            ];
            for (i, a) in distinct.iter().enumerate() {
                for b in &distinct[i + 1..] {
                    assert_ne!(cypher_value_eq(a, b), Some(true), "{a:?} vs {b:?}");
                    assert_ne!(h(a), h(b), "{a:?} and {b:?} share a bucket needlessly");
                }
            }
        }

        #[test]
        fn nan_null_and_null_bearing_containers_are_unhashable() {
            for value in [
                float(f64::NAN),
                ScalarValue::Float32(Some(f32::NAN)),
                ScalarValue::Int64(None),
                ScalarValue::Null,
                floats(&[f64::NAN]),
                list(&[int(1), ScalarValue::Int64(None)], &DataType::Int64),
                map(vec![("a", ScalarValue::Int64(None))]),
                map(vec![("a", float(f64::NAN))]),
                date_scalar(None),
            ] {
                assert_eq!(cypher_equality_hash(&value), None, "{value:?}");
            }
        }

        /// What `collect(DISTINCT)` did before it was bucketed: scan every kept value.
        fn naive_distinct(input: &[ScalarValue]) -> Vec<ScalarValue> {
            let mut kept: Vec<ScalarValue> = Vec::new();
            for v in input {
                if !kept
                    .iter()
                    .any(|seen| cypher_value_eq(seen, v) == Some(true))
                {
                    kept.push(v.clone());
                }
            }
            kept
        }

        fn indexed_distinct(input: &[ScalarValue]) -> Vec<ScalarValue> {
            let mut kept = Vec::new();
            let mut index = DistinctIndex::default();
            for v in input {
                index.admit(&mut kept, v.clone(), cypher_value_eq);
            }
            kept
        }

        #[test]
        fn index_matches_a_full_scan_over_the_corpus_in_any_order() {
            let corpus = corpus();
            let mut orders = vec![corpus.clone(), corpus.iter().rev().cloned().collect()];
            // Doubling every value exercises rejection of repeats.
            orders.push(corpus.iter().chain(corpus.iter()).cloned().collect());
            let mut state = 0x9e37_79b9_7f4a_7c15_u64;
            for _ in 0..8 {
                let mut shuffled = corpus.clone();
                for i in (1..shuffled.len()).rev() {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    shuffled.swap(i, usize::try_from(state >> 33).unwrap() % (i + 1));
                }
                orders.push(shuffled);
            }
            for order in &orders {
                assert_eq!(indexed_distinct(order), naive_distinct(order));
            }
        }

        #[test]
        fn index_matches_a_full_scan_over_a_dense_random_workload() {
            let mut state = 42_u64;
            let mut next = |bound: u64| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 33) % bound
            };
            let input: Vec<ScalarValue> = (0..3_000)
                .map(|_| {
                    let small = i64::try_from(next(12)).unwrap();
                    match next(9) {
                        0 => int(small),
                        1 => float(small as f64),
                        2 => float(small as f64 + 0.5),
                        3 => text(&small.to_string()),
                        4 => ints(&[small, small + 1]),
                        5 => floats(&[small as f64, f64::from(u8::try_from(next(3)).unwrap())]),
                        6 => node(next(12), small),
                        7 => relationship(next(12), small),
                        _ => map(vec![("k", int(small)), ("j", float(small as f64))]),
                    }
                })
                .collect();
            let kept = indexed_distinct(&input);
            assert_eq!(kept, naive_distinct(&input));
            // The workload really collapses: far fewer kept than offered.
            assert!(
                kept.len() * 10 < input.len(),
                "kept {} of {}",
                kept.len(),
                input.len()
            );
        }

        #[test]
        fn a_shared_bucket_is_resolved_by_equality_not_by_hash() {
            // A node, a relationship and a bare uuid with one identity are forced into
            // one bucket (the node and the relationship each equal the bare uuid), yet
            // a node never equals a relationship.
            let (n, r, u) = (node(1, 0), relationship(1, 0), uuid(1));
            assert_eq!(cypher_equality_hash(&n), cypher_equality_hash(&r));
            assert_eq!(cypher_value_eq(&n, &r), Some(false));
            assert_eq!(
                indexed_distinct(&[n.clone(), r.clone(), u.clone()]),
                vec![n, r]
            );

            // `i64::MAX` and `i64::MAX - 1` both equal the float 2^63 (equality goes
            // through f64) but not each other (it goes through i128), so they must
            // share a bucket and still both be kept.
            let (hi, lo, f) = (
                int(i64::MAX),
                int(i64::MAX - 1),
                float(9_223_372_036_854_775_808.0),
            );
            assert_eq!(cypher_value_eq(&hi, &f), Some(true));
            assert_eq!(cypher_value_eq(&lo, &f), Some(true));
            assert_eq!(cypher_value_eq(&hi, &lo), Some(false));
            assert_eq!(cypher_equality_hash(&hi), cypher_equality_hash(&lo));
            assert_eq!(indexed_distinct(&[hi.clone(), lo.clone(), f]), vec![hi, lo]);
        }

        #[test]
        fn unhashable_values_are_kept_and_never_deduplicated() {
            let nan = float(f64::NAN);
            let holey = list(&[int(1), ScalarValue::Int64(None)], &DataType::Int64);
            let input = vec![
                nan.clone(),
                nan.clone(),
                holey.clone(),
                holey.clone(),
                int(1),
            ];
            assert_eq!(indexed_distinct(&input), input);
            assert_eq!(naive_distinct(&input), input);
        }

        #[test]
        fn index_charges_memory_proportional_to_the_values_it_tracks() {
            let mut kept = Vec::new();
            let mut index = DistinctIndex::default();
            assert_eq!(index.heap_size(), 0);
            for n in 0..1_000 {
                index.admit(&mut kept, int(n), cypher_value_eq);
            }
            assert!(index.heap_size() >= 1_000 * std::mem::size_of::<usize>());
        }
    }
}
