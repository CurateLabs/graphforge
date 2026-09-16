//! Scalar builtin dispatch and execution UDFs.

use super::{
    CYPHER_REL_TYPE, CYPHER_TO_BOOLEAN, CYPHER_TO_FLOAT, CYPHER_TO_INTEGER, CYPHER_TO_STRING,
    PathNodeHydration, decode_het, is_het_struct_type, null_unless, resolve_path_builtin,
    unwrap_het, validate_heterogeneous_arguments,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, cast, lit, when,
};
use datafusion::scalar::ScalarValue;
use std::sync::{Arc, LazyLock};

/// Look up a Cypher built-in function name and produce the DataFusion call.
///
/// Returns `None` if the name is not in the built-in table.
#[allow(
    clippy::too_many_lines,
    reason = "a flat one-arm-per-builtin dispatch table; clearest kept inline"
)]
pub(super) fn resolve_builtin(
    name: &str,
    args: Vec<DfExpr>,
    path_hydration: impl FnOnce() -> Option<PathNodeHydration>,
) -> Option<DfExpr> {
    use datafusion::functions::math::expr_fn as mfn;
    use datafusion::functions::string::expr_fn as sfn;

    let name = name.to_ascii_lowercase();
    let mut a = args;
    match name.as_str() {
        // String functions
        "toupper" | "upper" => Some(sfn::upper(a.remove(0))),
        "tolower" | "lower" => Some(sfn::lower(a.remove(0))),
        "trim" => Some(sfn::btrim(a)),
        "ltrim" => Some(sfn::ltrim(a)),
        "rtrim" => Some(sfn::rtrim(a)),
        "string.concat" | "concat" => Some(sfn::concat(a)),
        "replace" => Some(sfn::replace(a.remove(0), a.remove(0), a.remove(0))),
        "substring" if a.len() == 2 || a.len() == 3 => Some(cypher_substring(a)),
        // Unambiguously a string character-count operation.
        "char_length" | "character_length" => Some(
            datafusion::functions::unicode::expr_fn::char_length(a.remove(0)),
        ),
        // Type conversion. `toString` routes through `cypher_to_string` so a
        // typed temporal (Date32/Time64/`localdatetime`/`time` struct) renders to
        // its canonical openCypher string; every other type falls back to a plain
        // `Utf8` cast (unchanged behaviour). The `datetime` renderer arm lands with
        // the `datetime`-struct migration. (ADR 0009)
        "tostring" => Some(CYPHER_TO_STRING.call(vec![a.remove(0)])),
        "tointeger" => Some(CYPHER_TO_INTEGER.call(vec![a.remove(0)])),
        "tofloat" => Some(CYPHER_TO_FLOAT.call(vec![a.remove(0)])),
        "toboolean" => Some(CYPHER_TO_BOOLEAN.call(vec![a.remove(0)])),

        // Runtime `date(<expr>)` (a non-constant argument; constants are handled
        // as a `Date32` scalar in `lower_temporal`). `to_date` already yields a
        // typed `Date32`, so dates round-trip as one type — e.g.
        // `date(toString(d)) = d` compares `Date32 == Date32` (ADR 0009). A
        // `Date32` argument passes through unchanged; an ISO string is parsed.
        "date" if a.len() == 1 => {
            use datafusion::functions::datetime::expr_fn::to_date;
            Some(to_date(vec![a.remove(0)]))
        }

        // Math functions
        "abs" => Some(mfn::abs(a.remove(0))),
        "ceil" => Some(mfn::ceil(a.remove(0))),
        "floor" => Some(mfn::floor(a.remove(0))),
        "round" => Some(mfn::round(a)),
        "sqrt" => Some(mfn::sqrt(a.remove(0))),
        "log" => Some(mfn::log(a.remove(0), a.remove(0))),
        "exp" => Some(mfn::exp(a.remove(0))),
        "power" => Some(mfn::power(a.remove(0), a.remove(0))),
        // `rand()` → a random float in [0, 1). Non-deterministic, but the
        // Quantifier9-12 invariant scenarios only use it to build a random
        // sub-list and then assert a result that holds for ANY list. (#955)
        "rand" if a.is_empty() => Some(mfn::random()),
        // `sign(n)` → -1 / 0 / 1 (openCypher returns an integer). Arity-guarded so
        // a wrong-arity call errors rather than panicking on `remove(0)`.
        "sign" if a.len() == 1 => Some(cast(mfn::signum(a.remove(0)), DataType::Int64)),
        // `coalesce(a, b, …)` → first non-null argument (at least one).
        "coalesce" if !a.is_empty() => Some(datafusion::functions::core::expr_fn::coalesce(a)),
        // `tail(list)` → every element but the first.
        "tail" if a.len() == 1 => Some(datafusion::functions_nested::expr_fn::array_pop_front(
            a.remove(0),
        )),
        // `split(str, delim)` → list of substrings.
        "split" if a.len() == 2 => Some(datafusion::functions_nested::expr_fn::string_to_array(
            a.remove(0),
            a.remove(0),
            DfExpr::Literal(ScalarValue::Utf8(None), None),
        )),

        // Cypher `length(<list>)` → element count. openCypher `length` is
        // path/list-oriented, so it maps cleanly to `array_length` (#709) — for
        // a variable-length edge list `r`, `length(r)` is the hop count per path.
        "length" => Some(datafusion::functions_nested::expr_fn::array_length(
            a.remove(0),
        )),

        // Cypher `size()` — element count of a list OR character count of a
        // string. Polymorphic, so it dispatches on the argument's runtime type
        // in `cypher_size` (a static `ScalarUDF`) rather than statically mapping
        // to `array_length` (which would mis-handle `size("str")`).
        "size" => Some(CYPHER_SIZE.call(vec![a.remove(0)])),

        // ---- list / relationship-list access (#743) ----
        // openCypher list indexing is 0-based with negative-from-end and
        // null-on-out-of-range; DataFusion `array_element` is 1-based (negatives
        // already count from the end, OOB → null), so a non-negative index is
        // shifted +1 and negatives pass through. See `one_based_index`.
        "_subscript" => {
            let list = a.remove(0);
            let idx = a.remove(0);
            Some(datafusion::functions_nested::expr_fn::array_element(
                list,
                one_based_index(idx),
            ))
        }

        // `head(list)` / `last(list)` — first / last element.
        "head" => Some(datafusion::functions_nested::expr_fn::array_element(
            a.remove(0),
            lit(1_i64),
        )),
        "last" => Some(datafusion::functions_nested::expr_fn::array_element(
            a.remove(0),
            lit(-1_i64),
        )),

        // `r[start..end]` slicing. The parser emits distinct internal function
        // names for omitted bounds so explicit `null` can propagate to a null
        // list (#962). openCypher: 0-based, start inclusive, end **exclusive**,
        // negatives from end; DataFusion
        // `array_slice(list, begin, end)` is 1-based and end **inclusive**.
        // Translation (per bound, via CASE on sign):
        //   begin: omitted → 1; s>=0 → s+1; s<0 → s (from end)
        //   end:   omitted → array_length(list); e>=0 → e (excl e == incl e-1,
        //          and 1-based incl == 0-based e-1, so the 1-based bound is just
        //          e); e<0 → e-1 (exclusive → inclusive shifts one toward start)
        "_slice" => {
            let list = a.remove(0);
            let start = a.remove(0);
            let end = a.remove(0);
            Some(cypher_slice(list, Some(start), Some(end)))
        }
        "_slice_from_start" => {
            let list = a.remove(0);
            let end = a.remove(0);
            Some(cypher_slice(list, None, Some(end)))
        }
        "_slice_to_end" => {
            let list = a.remove(0);
            let start = a.remove(0);
            Some(cypher_slice(list, Some(start), None))
        }

        // `range(start, end [, step])` uses the inclusive Cypher range UDF.
        // The runtime step controls direction and defaults to 1.
        "range" if a.len() == 2 || a.len() == 3 => {
            let from = a.remove(0);
            let end = a.remove(0);
            let by = if a.is_empty() {
                lit(1_i64)
            } else {
                a.remove(0)
            };
            Some(CYPHER_RANGE.call(vec![from, end, by]))
        }

        // `type(rel)` — the relation-type name. For a relationship-list element
        // (a `Struct<…, rel_type>`), read the `rel_type` field.
        "type" => Some(CYPHER_REL_TYPE.call(vec![a.remove(0)])),

        // Named-path internal builtins (#754) live in their own table.
        other => resolve_path_builtin(other, a, path_hydration),
    }
}

/// Build zero-based element ordinals for a list while preserving null versus
/// empty input (`null -> null`, `[] -> []`). Used by relationally lifted list
/// comprehensions whose element order must survive an unwind/regroup cycle.
pub(crate) fn list_index_range(list: DfExpr) -> DfExpr {
    let len = cast(
        datafusion::functions_nested::expr_fn::array_length(list),
        DataType::Int64,
    );
    CYPHER_RANGE.call(vec![lit(0_i64), len - lit(1_i64), lit(1_i64)])
}

fn cypher_substring(mut args: Vec<DfExpr>) -> DfExpr {
    let original = args.remove(0);
    let start = args.remove(0) + lit(1_i64);
    let substring = if args.is_empty() {
        datafusion::functions::unicode::expr_fn::substr(original, start)
    } else {
        datafusion::functions::unicode::expr_fn::substring(original, start, args.remove(0))
    };
    cast(substring, DataType::Utf8)
}

/// openCypher 0-based index → DataFusion `array_element` 1-based index.
///
/// `CASE WHEN idx >= 0 THEN idx + 1 ELSE idx END` — non-negative indices shift
/// up by one; negative indices already count from the end in both systems.
pub(super) fn one_based_index(idx: DfExpr) -> DfExpr {
    when(idx.clone().gt_eq(lit(0_i64)), idx.clone() + lit(1_i64))
        .otherwise(idx)
        .expect("CASE build is infallible for a single WHEN + ELSE")
}

/// Whether a lowered expression is the `Null` literal.
fn as_null_literal(e: &DfExpr) -> bool {
    matches!(e, DfExpr::Literal(ScalarValue::Null, _))
}

fn cypher_slice(list: DfExpr, start: Option<DfExpr>, end: Option<DfExpr>) -> DfExpr {
    let mut present = lit(true);
    let begin_expr = match start {
        Some(start) if as_null_literal(&start) => {
            present = lit(false);
            lit(1_i64)
        }
        Some(start) => {
            present = present.and(start.clone().is_not_null());
            when(start.clone().gt_eq(lit(0_i64)), start.clone() + lit(1_i64))
                .otherwise(start)
                .expect("CASE build")
        }
        None => lit(1_i64),
    };
    let end_expr = match end {
        Some(end) if as_null_literal(&end) => {
            present = lit(false);
            cast(
                datafusion::functions_nested::expr_fn::array_length(list.clone()),
                DataType::Int64,
            )
        }
        Some(end) => {
            present = present.and(end.clone().is_not_null());
            when(end.clone().gt_eq(lit(0_i64)), end.clone())
                .otherwise(end - lit(1_i64))
                .expect("CASE build")
        }
        None => cast(
            datafusion::functions_nested::expr_fn::array_length(list.clone()),
            DataType::Int64,
        ),
    };
    let slice =
        datafusion::functions_nested::expr_fn::array_slice(list, begin_expr, end_expr, None);
    null_unless(present, slice)
}

// ---------------------------------------------------------------------------
// cypher_size UDF
// ---------------------------------------------------------------------------

/// The Cypher `size()` scalar function: element count of a list **or** character
/// count of a string, dispatched on the argument's runtime type.
///
/// openCypher `size()` is polymorphic, so it cannot be statically mapped to a
/// single DataFusion function (`array_length` would mis-handle a string,
/// `char_length` a list). This UDF inspects the argument's [`DataType`] and
/// delegates to the matching Arrow kernel; an unsupported type yields `Null`.
///
/// Defined as a static [`ScalarUDF`] and invoked inline via
/// [`ScalarUDF::call`], so it carries its own implementation in the produced
/// `Expr` and needs no `SessionContext` registration.
static CYPHER_SIZE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherSize::new()));

/// Opaque per-row marker used beside literal-null grouping keys. DataFusion
/// otherwise removes the null key and changes an empty grouped aggregate into
/// a one-row global aggregate.
pub(crate) static CYPHER_ROW_MARKER: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherRowMarker::new()));

/// Identify the compiler-owned, always-true per-row marker by implementation.
/// Execution rewrites must not accept a user function with the same name.
#[must_use]
pub fn is_cypher_row_marker(function: &ScalarUDF) -> bool {
    function.inner().downcast_ref::<CypherRowMarker>().is_some()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherRowMarker {
    signature: Signature,
}

impl CypherRowMarker {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherRowMarker {
    fn name(&self) -> &'static str {
        "cypher_row_marker"
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
        validate_heterogeneous_arguments(&args.args)?;
        let values = datafusion::arrow::array::BooleanArray::from(vec![true; args.number_rows]);
        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherSize {
    signature: Signature,
}

impl CypherSize {
    fn new() -> Self {
        // One argument of any type; immutable (same input → same output).
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherSize {
    fn name(&self) -> &'static str {
        "cypher_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, Int64Array};
        use datafusion::arrow::compute::kernels::length::length;
        use datafusion::common::cast::{as_large_list_array, as_list_array};

        validate_heterogeneous_arguments(&args.args)?;
        let array = args.args[0].to_array(args.number_rows)?;
        let out: Int64Array = match array.data_type() {
            // Element count per list row (null lists → null).
            DataType::List(_) => {
                let list = as_list_array(&array)?;
                (0..list.len())
                    .map(|i| {
                        (!list.is_null(i))
                            .then(|| i64::try_from(list.value(i).len()).unwrap_or(i64::MAX))
                    })
                    .collect()
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array)?;
                (0..list.len())
                    .map(|i| {
                        (!list.is_null(i))
                            .then(|| i64::try_from(list.value(i).len()).unwrap_or(i64::MAX))
                    })
                    .collect()
            }
            // Character/byte count for strings — Arrow's `length` kernel returns
            // the count as an integer array; cast to Int64 for a uniform return.
            DataType::Utf8 | DataType::LargeUtf8 => {
                let lengths = length(&array)?;
                let casted = datafusion::arrow::compute::cast(&lengths, &DataType::Int64)?;
                casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("cast to Int64 yields Int64Array")
                    .clone()
            }
            // A heterogeneous tagged element (ADR 0011) — e.g. the loop variable
            // of `none(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3)`. Decode per
            // row and count a list payload's elements or a string payload's
            // bytes; any other payload falls through to null like the untyped
            // arm below.
            t if is_het_struct_type(Some(t)) => (0..array.len())
                .map(|i| {
                    let sv = ScalarValue::try_from_array(&array, i).ok()?;
                    match decode_het(&sv)? {
                        ScalarValue::List(l) => (!l.is_null(0))
                            .then(|| i64::try_from(l.value(0).len()).unwrap_or(i64::MAX)),
                        ScalarValue::LargeList(l) => (!l.is_null(0))
                            .then(|| i64::try_from(l.value(0).len()).unwrap_or(i64::MAX)),
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            i64::try_from(s.len()).ok()
                        }
                        _ => None,
                    }
                })
                .collect(),
            // Unsupported argument type → all-null (Cypher `size` of a non-
            // list/string is undefined; null is the lenient choice).
            _ => (0..array.len()).map(|_| None).collect(),
        };
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// cypher_reverse UDF
// ---------------------------------------------------------------------------

/// `reverse(x)` for an argument whose type is unknown at plan time (e.g. a
/// parameter or an unresolved property). Dispatches at runtime: a string
/// reverses its characters, a list its elements. The known-string / known-list
/// cases are routed directly to `unicode::reverse` / `array_reverse` at lowering
/// and never reach this UDF. (#955)
pub(super) static CYPHER_REVERSE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherReverse::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherReverse {
    signature: Signature,
}

impl CypherReverse {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherReverse {
    fn name(&self) -> &'static str {
        "cypher_reverse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // Same type in, same type out.
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{
            Array, LargeStringArray, ListArray, StringArray, UInt32Array,
        };
        use datafusion::common::cast::as_list_array;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        validate_heterogeneous_arguments(&args.args)?;
        let array = args.args[0].to_array(args.number_rows)?;
        match array.data_type() {
            DataType::Utf8 => {
                let s = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("cypher_reverse: not a string array".into())
                    })?;
                let out: StringArray = (0..s.len())
                    .map(|i| (!s.is_null(i)).then(|| s.value(i).chars().rev().collect::<String>()))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            DataType::LargeUtf8 => {
                let s = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("cypher_reverse: not a large string array".into())
                    })?;
                let out: LargeStringArray = (0..s.len())
                    .map(|i| (!s.is_null(i)).then(|| s.value(i).chars().rev().collect::<String>()))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            DataType::List(_) => {
                let list = as_list_array(&array)?;
                let values = list.values();
                let offsets = list.offsets();
                // Per row, emit element indices in reverse — the row's length is
                // unchanged, so the original offsets/nulls are reused verbatim.
                let mut idx: Vec<u32> = Vec::with_capacity(values.len());
                for w in offsets.windows(2) {
                    let (start, end) = (w[0], w[1]);
                    for j in (start..end).rev() {
                        idx.push(u32::try_from(j).unwrap_or(0));
                    }
                }
                let taken =
                    datafusion::arrow::compute::take(values, &UInt32Array::from(idx), None)?;
                let field = match list.data_type() {
                    DataType::List(f) => Arc::clone(f),
                    _ => unreachable!("matched List above"),
                };
                let reversed = ListArray::new(field, offsets.clone(), taken, list.nulls().cloned());
                Ok(ColumnarValue::Array(Arc::new(reversed)))
            }
            other => Err(DataFusionError::Plan(format!(
                "reverse() expects a string or list, got {other:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Cypher boolean / predicate UDFs
// ---------------------------------------------------------------------------

pub(super) static CYPHER_STARTS_WITH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherStringPredicate::new(StringPredicate::Starts)));
pub(super) static CYPHER_ENDS_WITH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherStringPredicate::new(StringPredicate::Ends)));
pub(super) static CYPHER_CONTAINS: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherStringPredicate::new(StringPredicate::Contains))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StringPredicate {
    Starts,
    Ends,
    Contains,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherStringPredicate {
    signature: Signature,
    kind: StringPredicate,
}

impl CypherStringPredicate {
    fn new(kind: StringPredicate) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            kind,
        }
    }
}

impl ScalarUDFImpl for CypherStringPredicate {
    fn name(&self) -> &'static str {
        match self.kind {
            StringPredicate::Starts => "cypher_starts_with",
            StringPredicate::Ends => "cypher_ends_with",
            StringPredicate::Contains => "cypher_contains",
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
                let l = ScalarValue::try_from_array(&lhs, i).ok()?;
                let r = ScalarValue::try_from_array(&rhs, i).ok()?;
                let l = scalar_as_string(&l)?;
                let r = scalar_as_string(&r)?;
                Some(match self.kind {
                    StringPredicate::Starts => l.starts_with(&r),
                    StringPredicate::Ends => l.ends_with(&r),
                    StringPredicate::Contains => l.contains(&r),
                })
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

fn scalar_as_string(s: &ScalarValue) -> Option<String> {
    let s = unwrap_het(s.clone());
    if s.is_null() {
        return None;
    }
    match s {
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v,
        _ => None,
    }
}

static CYPHER_RANGE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherRange::new()));
#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherRange {
    signature: Signature,
}

impl CypherRange {
    fn new() -> Self {
        Self {
            // Keep literal invalid-argument cases in the runtime phase; the TCK
            // asserts `range()` argument errors at runtime, and constant-folding
            // would otherwise wrap them as DataFusion planning failures.
            signature: Signature::any(3, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for CypherRange {
    fn name(&self) -> &'static str {
        "cypher_range"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(DataType::Int64, true))
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Int64Builder, ListBuilder};
        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let starts = args.args[0].to_array(rows)?;
        let ends = args.args[1].to_array(rows)?;
        let steps = args.args[2].to_array(rows)?;
        let mut out = ListBuilder::new(Int64Builder::new());
        for i in 0..rows {
            let start = ScalarValue::try_from_array(&starts, i)?;
            let end = ScalarValue::try_from_array(&ends, i)?;
            let step = ScalarValue::try_from_array(&steps, i)?;
            if start.is_null() || end.is_null() || step.is_null() {
                out.append_null();
                continue;
            }
            let start = scalar_as_i64_arg(&start, "range start")?;
            let end = scalar_as_i64_arg(&end, "range end")?;
            let step = scalar_as_i64_arg(&step, "range step")?;
            if step == 0 {
                return Err(datafusion::error::DataFusionError::Plan(
                    "range step must not be zero".into(),
                ));
            }
            if (step > 0 && start > end) || (step < 0 && start < end) {
                out.append(true);
                continue;
            }
            let mut cur = start;
            loop {
                out.values().append_value(cur);
                if cur == end {
                    break;
                }
                let Some(next) = cur.checked_add(step) else {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "range overflowed i64".into(),
                    ));
                };
                if (step > 0 && next > end) || (step < 0 && next < end) {
                    break;
                }
                cur = next;
            }
            out.append(true);
        }
        Ok(ColumnarValue::Array(std::sync::Arc::new(out.finish())))
    }
}

fn scalar_as_i64_arg(s: &ScalarValue, name: &str) -> datafusion::error::Result<i64> {
    match s {
        ScalarValue::Int8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!("{name} exceeds i64::MAX"))
        }),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "{name} must be an integer, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests;
