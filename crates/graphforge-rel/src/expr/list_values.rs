//! Heterogeneous list construction and concatenation.
//!
//! Nested scalar, list, map, and graph payloads use the existing value codec.
//! Parent lowering dispatch and graph-shape ownership remain unchanged.

use super::{
    Arc, Array, ColumnarValue, DataType, DfExpr, ExprSchemable, Field, FieldRef, LazyLock,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, ScalarValue, Signature, Volatility, decode_het,
    decoded_scalar_at, graph_value_types_compatible, is_date_struct, is_datetime_struct,
    is_duration_struct, is_het_struct_type, is_localdatetime_struct, is_time_struct,
    new_empty_array, unify_graph_value_nullability, validate_heterogeneous_arguments,
};
use datafusion::logical_expr::ReturnFieldArgs;

#[cfg(test)]
mod tests;

/// Resolve a lowered list element to a constant `ScalarValue` — a literal, or a
/// `named_struct(...)` map whose keys are string literals and whose values are
/// themselves constant (recursively). `None` for a non-constant element. Lets a
/// list literal that mixes maps with scalars/containers fold every element and
/// reach the tagged het path without folding maps everywhere (#1005).
fn try_const_scalar(e: &DfExpr) -> Option<ScalarValue> {
    match e {
        DfExpr::Literal(s, _) => Some(s.clone()),
        DfExpr::ScalarFunction(f) if f.func.name() == "named_struct" => {
            let mut entries: Vec<(String, ScalarValue)> = Vec::with_capacity(f.args.len() / 2);
            let pairs = f.args.chunks_exact(2);
            if !pairs.remainder().is_empty() {
                return None;
            }
            for pair in pairs {
                let DfExpr::Literal(ScalarValue::Utf8(Some(k)), _) = &pair[0] else {
                    return None;
                };
                entries.push((k.clone(), try_const_scalar(&pair[1])?));
            }
            const_map_scalar(&entries)
        }
        _ => None,
    }
}

/// Build a constant map `{k: v, …}` as a `ScalarValue::Struct` from constant
/// entries — the exact `Struct` shape `named_struct` produces (each field
/// nullable), so `m.k` access, equality, and rendering are unchanged.
pub(super) fn const_map_scalar(entries: &[(String, ScalarValue)]) -> Option<ScalarValue> {
    use datafusion::arrow::array::{ArrayRef, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};
    use std::sync::Arc;
    if entries.is_empty() {
        return Some(ScalarValue::Struct(Arc::new(
            StructArray::new_empty_fields(1, None),
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(entries.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(entries.len());
    for (k, sv) in entries {
        let arr = sv.to_array().ok()?; // length-1 array
        fields.push(Field::new(k, arr.data_type().clone(), true));
        arrays.push(arr);
    }
    let s = StructArray::try_new(Fields::from(fields), arrays, None).ok()?;
    Some(ScalarValue::Struct(Arc::new(s)))
}

/// Whether a `Struct` value is a plain Cypher MAP — three-valued structural value
/// — rather than one of the reserved struct shapes that must NOT be encoded as a
/// het map element (#1005): a het-tagged element, a node/relationship/path entity,
/// or a typed temporal value (`date`/`time`/`localdatetime`/`datetime`/`duration`).
fn is_plain_map_struct(arr: &datafusion::arrow::array::StructArray) -> bool {
    use datafusion::arrow::array::Array;
    is_plain_map_struct_type(arr.data_type())
}

/// [`is_plain_map_struct`] on a `DataType` — a `Struct` that is a plain Cypher map,
/// not a het-tagged element, a typed temporal value, or a node/relationship/path
/// entity. Used to route `m.k` on a map-typed column to `get_field` (#1017).
pub(super) fn is_plain_map_struct_type(dt: &DataType) -> bool {
    let DataType::Struct(fields) = dt else {
        return false;
    };
    // Reserved entity field names (mirror `is_entity_struct`).
    let is_entity = fields.iter().any(|f| {
        matches!(
            f.name().as_str(),
            "node_uuid" | "src_uuid" | "dst_uuid" | "nodes" | "relationships" | "labels"
        )
    });
    !is_entity
        && !is_het_struct_type(Some(dt))
        && !is_date_struct(dt)
        && !is_localdatetime_struct(dt)
        && !is_duration_struct(dt)
        && !is_time_struct(dt)
        && !is_datetime_struct(dt)
}

/// Arrow fields of a tagged heterogeneous list element (ADR 0010/0011) that can
/// nest to `depth` levels. `__het_key` is first (native `Struct` min/max orders
/// flat numeric lists by value — ADR 0010). `__het_tag`: 0=int, 1=float, 2=str,
/// 3=bool, 4=list, 5=map. For `depth >= 1` a `__het_list: List<Struct{…depth-1…}>`
/// field holds a nested-list element's tagged children and a `__het_map:
/// List<Struct{__het_mkey, __het_mval: Struct{…depth-1…}}>` field holds a map
/// element's key/tagged-value entries — a *distinct, shallower, finite* type per
/// level (recursion by value, not a recursive Arrow type; the literal's depth is
/// known at lowering time — ADR 0011).
pub(super) fn het_fields(depth: usize) -> datafusion::arrow::datatypes::Fields {
    graphforge_value::heterogeneous::constant_fields(depth)
}

/// The nesting depth of a value as a het element: a scalar is `0`, a list or map
/// is `1 + max child depth` (empty container = 1). `None` if the value cannot be a
/// het element (a node/relationship/path entity or a typed temporal value).
pub(super) fn het_depth(s: &ScalarValue) -> Option<usize> {
    use datafusion::arrow::array::Array;
    let s = unwrap_het(s.clone());
    match &s {
        ScalarValue::Int64(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Utf8(_)
        | ScalarValue::LargeUtf8(_)
        | ScalarValue::Utf8View(_)
        | ScalarValue::Boolean(_)
        | ScalarValue::Null => Some(0),
        ScalarValue::List(arr) => {
            let inner = arr.value(0);
            let mut d = 0;
            for i in 0..inner.len() {
                // An inner list that is itself heterogeneous was already lowered to
                // a tagged struct (bottom-up lowering); unwrap it back to the plain
                // value so depth/encoding are computed uniformly.
                let e = unwrap_het(ScalarValue::try_from_array(&inner, i).ok()?);
                d = d.max(het_depth(&e)?);
            }
            Some(1 + d)
        }
        // A plain map (#1005): 1 + the deepest value; an empty map is depth 1. A
        // value already tagged (a het list value) unwraps back to its plain form.
        ScalarValue::Struct(arr) if is_plain_map_struct(arr) => {
            let mut d = 0;
            for i in 0..arr.num_columns() {
                let v = unwrap_het(ScalarValue::try_from_array(arr.column(i), 0).ok()?);
                d = d.max(het_depth(&v)?);
            }
            Some(1 + d)
        }
        _ => None,
    }
}

/// Decode an already-tagged het element back to its plain value (for re-encoding
/// uniformly at an outer level); leaves a non-tagged value unchanged.
pub(super) fn unwrap_het(s: ScalarValue) -> ScalarValue {
    if let ScalarValue::Dictionary(_, value) = s {
        return unwrap_het(*value);
    }
    decode_het(&s).unwrap_or(s)
}

/// Build the tagged-struct array for `scalars`, every element encoded uniformly at
/// `depth` (ADR 0011). List elements recurse their children at `depth - 1`.
#[allow(
    clippy::too_many_lines,
    reason = "one cohesive per-field array builder; splitting it would obscure the field/offset bookkeeping"
)]
pub(super) fn build_het_struct(
    scalars: &[ScalarValue],
    depth: usize,
) -> Option<datafusion::arrow::array::StructArray> {
    let values = scalars
        .iter()
        .map(heterogeneous_literal)
        .collect::<Option<Vec<_>>>()?;
    graphforge_value::heterogeneous::encode_constant(&values, depth).ok()
}

// DataFusion conversion stays here; layout/tag interpretation belongs to value.
fn heterogeneous_literal(scalar: &ScalarValue) -> Option<graphforge_value::Literal> {
    use graphforge_value::Literal;
    let scalar = unwrap_het(scalar.clone());
    if scalar.is_null() {
        return Some(Literal::Null);
    }
    Some(match &scalar {
        ScalarValue::Int64(Some(value)) => Literal::Int(*value),
        ScalarValue::Float64(Some(value)) => Literal::Float(*value),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Literal::Str(value.clone()),
        ScalarValue::Boolean(Some(value)) => Literal::Bool(*value),
        ScalarValue::List(values) => {
            let values = values.value(0);
            Literal::List(
                (0..values.len())
                    .map(|row| {
                        heterogeneous_literal(&ScalarValue::try_from_array(&values, row).ok()?)
                    })
                    .collect::<Option<Vec<_>>>()?,
            )
        }
        ScalarValue::Struct(values) if is_plain_map_struct(values) => Literal::Map(
            values
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    Some((
                        field.name().clone(),
                        heterogeneous_literal(
                            &ScalarValue::try_from_array(values.column(index), 0).ok()?,
                        )?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
        ),
        _ => return None,
    })
}

/// Build a heterogeneous list literal as the ADR-0010/0011 tagged struct
/// (`List<Struct{__het_*}>`) when `scalars` is a constant list that cannot be a
/// homogeneous Arrow array — a flat mix of `int`/`float`/`string`/`bool`, or a
/// list with nested-list elements (`[1, [1, 2]]`). Returns `None` if any element
/// is a map/struct/entity (deferred to a later ADR-0011 slice) — those fall to
/// `make_array`. Only called after the homogeneous const-fold has been ruled out,
/// so homogeneous lists keep their primitive `new_list` representation untouched.
fn tagged_numeric_list(scalars: &[ScalarValue]) -> Option<DfExpr> {
    use datafusion::arrow::array::ListArray;
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    // Every element must be het-representable (scalar or nested list); compute the
    // literal's nesting depth so the per-level struct types are finite and exact.
    let mut depth = 0usize;
    for s in scalars {
        depth = depth.max(het_depth(s)?);
    }
    let n = scalars.len();
    let elem = build_het_struct(scalars, depth)?;
    let list_field = Arc::new(Field::new(
        "item",
        DataType::Struct(het_fields(depth)),
        true,
    ));
    let list = ListArray::new(
        list_field,
        OffsetBuffer::from_lengths([n]),
        Arc::new(elem),
        None,
    );
    Some(DfExpr::Literal(ScalarValue::List(Arc::new(list)), None))
}

static CYPHER_DYNAMIC_HET_LIST: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDynamicHetList::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDynamicHetList {
    signature: Signature,
}

impl CypherDynamicHetList {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

fn dynamic_het_type(arg_types: &[DataType]) -> DataType {
    DataType::new_list(
        DataType::Struct(graphforge_value::heterogeneous::dynamic_fields(arg_types)),
        true,
    )
}

impl ScalarUDFImpl for CypherDynamicHetList {
    fn name(&self) -> &'static str {
        "cypher_dynamic_het_list"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(dynamic_het_type(arg_types))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            dynamic_het_type(&arg_types),
            false,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        validate_heterogeneous_arguments(&args.args)?;
        if args.args.len() > 127 {
            return Err(datafusion::error::DataFusionError::Plan(
                "heterogeneous list literal exceeds 127 elements".into(),
            ));
        }
        let DataType::List(item) = args.return_field.data_type() else {
            return Err(datafusion::error::DataFusionError::Internal(
                "dynamic heterogeneous list has a non-list return type".into(),
            ));
        };
        if !matches!(item.data_type(), DataType::Struct(_)) {
            return Err(datafusion::error::DataFusionError::Internal(
                "dynamic heterogeneous list has a non-struct element type".into(),
            ));
        }
        let values = args
            .args
            .iter()
            .map(|value| value.to_array(args.number_rows))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let list = graphforge_value::heterogeneous::encode_dynamic(&values, args.number_rows)
            .map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))?;
        if list.data_type() != args.return_field.data_type() {
            return Err(datafusion::error::DataFusionError::External(Box::new(
                graphforge_value::heterogeneous::ValueError::Schema,
            )));
        }
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

pub(super) fn lower_list_literal(
    elems: Vec<DfExpr>,
    input_schema: Option<&datafusion::common::DFSchema>,
) -> DfExpr {
    // Resolve every element to a constant if possible (a literal, or a const map
    // folded from `named_struct`, #1005). `None` if any element is non-constant.
    if let Some(scalars) = elems
        .iter()
        .map(try_const_scalar)
        .collect::<Option<Vec<ScalarValue>>>()
    {
        // Element type: the first non-null element's type, else Int64.
        let elem_type = scalars
            .iter()
            .map(ScalarValue::data_type)
            .find(|t| *t != DataType::Null)
            .unwrap_or(DataType::Int64);
        // Re-type untyped `Null`s to `elem_type` so the list can be a nullable array
        // of that type (`[1, null]` → `Int64[1, null]`). Without this, `new_list`
        // panics building a homogeneous array from an untyped null.
        let typed: Vec<ScalarValue> = scalars
            .iter()
            .map(|s| {
                if matches!(s, ScalarValue::Null) {
                    ScalarValue::try_from(&elem_type).unwrap_or(ScalarValue::Null)
                } else {
                    s.clone()
                }
            })
            .collect();
        // Const-fold a HOMOGENEOUS list to a single `ScalarValue::List` — including
        // a same-shape all-map list, which stays a PLAIN `List<Struct>` (so `x.field`
        // access in a quantifier resolves, #1004) AND is a literal (so it can nest
        // inside an outer tagged het list, #1005).
        if typed.iter().all(|s| s.data_type() == elem_type) {
            let list = ScalarValue::new_list(&typed, &elem_type, true);
            return DfExpr::Literal(ScalarValue::List(list), None);
        }
        // A list whose elements are ALL maps (with any nulls) keeps each element a
        // PLAIN map so `x.field` access in a quantifier resolves (#1004): a
        // DIFFERENT-shape all-map list is padded to the union of keys (missing key
        // → null) into a homogeneous `List<Struct>` literal — which `make_array`
        // itself cannot unify. Only a genuinely MIXED list (maps alongside
        // scalars/lists) uses the tagged het path, where map elements carry no
        // accessible fields. (#1005)
        let all_maps = scalars
            .iter()
            .any(|s| matches!(s, ScalarValue::Struct(a) if is_plain_map_struct(a)))
            && scalars.iter().all(|s| {
                s.is_null() || matches!(s, ScalarValue::Struct(a) if is_plain_map_struct(a))
            });
        if all_maps {
            if let Some(padded) = all_map_union_list(&scalars) {
                return padded;
            }
        } else if let Some(tagged) = tagged_numeric_list(&scalars) {
            return tagged;
        }
    }
    if let Some(schema) = input_schema
        && let Some(types) = elems
            .iter()
            .map(|elem| elem.get_type(schema).ok())
            .collect::<Option<Vec<_>>>()
        && types.windows(2).any(|pair| pair[0] != pair[1])
    {
        return CYPHER_DYNAMIC_HET_LIST.call(elems);
    }
    datafusion::functions_nested::expr_fn::make_array(elems)
}

pub(super) static CYPHER_LIST_PLUS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherListPlus::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherListPlus {
    signature: Signature,
}

impl CypherListPlus {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherListPlus {
    fn name(&self) -> &'static str {
        "cypher_list_plus"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        if list_plus_has_graph_value(arg_types) {
            Ok(list_plus_return_type(arg_types))
        } else {
            Ok(DataType::new_list(
                DataType::Struct(het_fields(list_plus_depth(arg_types))),
                true,
            ))
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "list/list and list/element shaping share one offset and validity pass"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, ArrayRef, ListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let left = args.args[0].to_array(rows)?;
        let right = args.args[1].to_array(rows)?;
        let left_is_list = list_item_type(left.data_type()).is_some();
        let right_is_list = list_item_type(right.data_type()).is_some();
        if !left_is_list && !right_is_list {
            return Err(DataFusionError::Execution(
                "list + requires at least one list operand".into(),
            ));
        }

        if left_is_list
            && !right_is_list
            && let Some(result) =
                invoke_tagged_list_element_plus(&left, &right, args.return_field.data_type())?
        {
            return Ok(ColumnarValue::Array(result));
        }

        let mut flat: Vec<ScalarValue> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        let mut validity: Vec<bool> = Vec::with_capacity(rows);
        offsets.push(0);

        for row in 0..rows {
            let row_values = match (left_is_list, right_is_list) {
                (true, true) => match (
                    list_elements_at(&left, row)?,
                    list_elements_at(&right, row)?,
                ) {
                    (Some(mut l), Some(r)) => {
                        l.extend(r);
                        Some(l)
                    }
                    _ => None,
                },
                (true, false) => match list_elements_at(&left, row)? {
                    Some(mut l) => {
                        let r = decoded_scalar_at(&right, row)?;
                        if let Some(r) = scalar_list_elements(&r)? {
                            l.extend(r);
                        } else {
                            l.push(r);
                        }
                        Some(l)
                    }
                    None => None,
                },
                (false, true) => match list_elements_at(&right, row)? {
                    Some(mut r) => {
                        let l = decoded_scalar_at(&left, row)?;
                        let mut l = scalar_list_elements(&l)?.unwrap_or_else(|| vec![l]);
                        l.append(&mut r);
                        Some(l)
                    }
                    None => None,
                },
                (false, false) => unreachable!("checked above"),
            };

            match row_values {
                Some(values) => {
                    flat.extend(values);
                    validity.push(true);
                }
                None => validity.push(false),
            }
            offsets.push(i32::try_from(flat.len()).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus: list too long".into())
            })?);
        }

        let DataType::List(field) = args.return_field.data_type() else {
            return Err(DataFusionError::Internal(
                "cypher_list_plus return type is not a list".into(),
            ));
        };
        if is_het_struct_type(Some(field.data_type()))
            && !is_dynamic_variant_struct(field.data_type())
        {
            let depth = list_plus_depth(&[left.data_type().clone(), right.data_type().clone()]);
            let values = build_het_struct(&flat, depth).ok_or_else(|| {
                DataFusionError::Execution(
                    "cypher_list_plus: cannot encode value in heterogeneous list".into(),
                )
            })?;
            let out = ListArray::new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                Arc::new(values) as ArrayRef,
                Some(NullBuffer::from(validity)),
            );
            return Ok(ColumnarValue::Array(Arc::new(out)));
        }
        if !is_dynamic_variant_struct(field.data_type()) {
            let flat = flat
                .into_iter()
                .map(|value| {
                    if value.data_type() == *field.data_type() {
                        Ok(value)
                    } else {
                        value.cast_to(field.data_type())
                    }
                })
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            let values = if flat.is_empty() {
                new_empty_array(field.data_type())
            } else {
                ScalarValue::iter_to_array(flat)?
            };
            let out = ListArray::new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                Some(NullBuffer::from(validity)),
            );
            return Ok(ColumnarValue::Array(Arc::new(out)));
        }
        let DataType::Struct(fields) = field.data_type() else {
            return Err(DataFusionError::Internal(
                "cypher_list_plus element type is not tagged".into(),
            ));
        };
        let variants = fields
            .iter()
            .filter(|field| {
                field
                    .name()
                    .starts_with(graphforge_value::heterogeneous::DYNAMIC_PREFIX)
            })
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let mut tags = Vec::with_capacity(flat.len());
        let mut valid = Vec::with_capacity(flat.len());
        let mut columns = Vec::with_capacity(variants.len() + 1);
        for value in &flat {
            let tag = variants
                .iter()
                .position(|variant| {
                    value.data_type() == *variant
                        || graph_value_types_compatible(&value.data_type(), variant)
                })
                .or_else(|| value.is_null().then_some(0))
                .ok_or_else(|| {
                    DataFusionError::External(Box::new(
                        graphforge_value::heterogeneous::ValueError::Kind,
                    ))
                })?;
            tags.push(i8::try_from(tag).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus has too many value variants".into())
            })?);
            valid.push(!value.is_null());
        }
        columns.push(Arc::new(datafusion::arrow::array::Int8Array::from(tags.clone())) as ArrayRef);
        for (variant_index, variant) in variants.iter().enumerate() {
            let null = ScalarValue::try_new_null(variant)?;
            let values = flat.iter().zip(&tags).map(|(value, tag)| {
                if usize::try_from(*tag).ok() == Some(variant_index) {
                    value.clone()
                } else {
                    null.clone()
                }
            });
            columns.push(ScalarValue::iter_to_array(values)?);
        }
        let tags = columns.remove(0);
        let tags = tags
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int8Array>()
            .ok_or_else(|| DataFusionError::Internal("dynamic tag column is not Int8".into()))?
            .clone();
        let values = graphforge_value::heterogeneous::encode_dynamic_rows(
            tags,
            columns,
            Some(NullBuffer::from(valid)),
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let out = ListArray::new(
            field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(values) as ArrayRef,
            Some(NullBuffer::from(validity)),
        );
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Append a tagged heterogeneous element to each list row without round-tripping
/// every existing element through [`ScalarValue`]. A runtime element whose tag is
/// itself a list still uses Cypher's dynamic list concatenation semantics; only
/// those nested children are promoted to the enclosing tagged depth.
#[allow(
    clippy::too_many_lines,
    reason = "one range-assembly pass keeps offsets, validity, and three Arrow sources synchronized"
)]
fn invoke_tagged_list_element_plus(
    left: &datafusion::arrow::array::ArrayRef,
    right: &datafusion::arrow::array::ArrayRef,
    return_type: &DataType,
) -> datafusion::error::Result<Option<datafusion::arrow::array::ArrayRef>> {
    use arrow_data::transform::MutableArrayData;
    use datafusion::arrow::array::{Array, ListArray, StructArray, make_array};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::error::ArrowError;
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    let Some(left) = left.as_any().downcast_ref::<ListArray>() else {
        return Ok(None);
    };
    let Some(right) = right.as_any().downcast_ref::<StructArray>() else {
        return Ok(None);
    };
    let DataType::List(return_field) = return_type else {
        return Ok(None);
    };
    if left.value_type() != right.data_type().clone()
        || return_field.data_type() != right.data_type()
        || !is_het_struct_type(Some(right.data_type()))
    {
        return Ok(None);
    }

    let Some(nested) = right
        .column_by_name(graphforge_value::heterogeneous::LIST)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
    else {
        return Ok(None);
    };

    let nested_values = nested.values();
    let nested_offsets = nested.value_offsets();
    let mut promoted_ranges = vec![None; right.len()];
    for row in 0..right.len() {
        if !matches!(
            graphforge_value::heterogeneous::decode_row(right, row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            graphforge_value::heterogeneous::Decoded::Payload(value)
                if value.data_type() == nested.data_type()
        ) {
            continue;
        }
        let start = usize::try_from(nested_offsets[row]).map_err(|_| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::ComputeError(
                    "negative heterogeneous-list offset".into(),
                )),
                None,
            )
        })?;
        let end = usize::try_from(nested_offsets[row + 1]).map_err(|_| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::ComputeError(
                    "negative heterogeneous-list offset".into(),
                )),
                None,
            )
        })?;
        promoted_ranges[row] = Some((start, end));
    }
    let promoted = if nested_values.is_empty() {
        Arc::new(right.slice(0, 0)) as datafusion::arrow::array::ArrayRef
    } else {
        promote_het_array(nested_values, right.data_type())?
    };

    let left_data = left.values().to_data();
    let right_data = right.to_data();
    let promoted_data = promoted.to_data();
    let capacity = left.values().len() + right.len() + promoted.len();
    let mut values = MutableArrayData::new(
        vec![&left_data, &right_data, &promoted_data],
        true,
        capacity,
    );
    let left_offsets = left.value_offsets();
    let mut offsets = Vec::with_capacity(left.len() + 1);
    let mut validity = Vec::with_capacity(left.len());
    let mut output_len = 0usize;
    offsets.push(0i32);
    for row in 0..left.len() {
        if left.is_null(row) {
            validity.push(false);
            offsets.push(i32::try_from(output_len).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus: list too long".into())
            })?);
            continue;
        }
        validity.push(true);
        let start = usize::try_from(left_offsets[row]).map_err(|_| {
            DataFusionError::Execution("cypher_list_plus: negative list offset".into())
        })?;
        let end = usize::try_from(left_offsets[row + 1]).map_err(|_| {
            DataFusionError::Execution("cypher_list_plus: negative list offset".into())
        })?;
        values.extend(0, start, end);
        output_len += end - start;
        if let Some((start, end)) = promoted_ranges[row] {
            values.extend(2, start, end);
            output_len += end - start;
        } else {
            values.extend(1, row, row + 1);
            output_len += 1;
        }
        offsets.push(
            i32::try_from(output_len).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus: list too long".into())
            })?,
        );
    }

    let values = make_array(values.freeze());
    Ok(Some(Arc::new(ListArray::new(
        return_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        Some(NullBuffer::from(validity)),
    ))))
}

/// Promote a tagged value array to a deeper version of the same recursive
/// heterogeneous schema. Existing buffers are reused; only missing deeper
/// list/map fields are introduced as null arrays.
fn promote_het_array(
    source: &datafusion::arrow::array::ArrayRef,
    target: &DataType,
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::{Array, ListArray, StructArray, new_null_array};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    if source.data_type() == target {
        return Ok(source.clone());
    }
    match (source.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            let source = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "heterogeneous value has a non-struct physical array".into(),
                    )
                })?;
            let columns = target_fields
                .iter()
                .map(|field| {
                    source.column_by_name(field.name()).map_or_else(
                        || Ok(new_null_array(field.data_type(), source.len())),
                        |column| promote_het_array(column, field.data_type()),
                    )
                })
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
            )))
        }
        (DataType::List(_), DataType::List(target_field)) => {
            let source = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Internal("heterogeneous list has a non-list physical array".into())
            })?;
            let values = promote_het_array(source.values(), target_field.data_type())?;
            Ok(Arc::new(ListArray::new(
                target_field.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )))
        }
        _ => Ok(cast(source, target)?),
    }
}

fn list_plus_return_type(arg_types: &[DataType]) -> DataType {
    let value_types = arg_types
        .iter()
        .flat_map(|arg_type| {
            let value_type = list_item_type(arg_type).unwrap_or(arg_type);
            dynamic_variant_types(value_type)
        })
        .filter(|value_type| !matches!(value_type, DataType::Null))
        .collect::<Vec<_>>();
    if let Some(first) = value_types.first()
        && is_graph_value_struct(first)
        && value_types
            .iter()
            .all(|value_type| graph_value_types_compatible(first, value_type))
    {
        // Widen nested nullability across variants so invoke-time
        // `ScalarValue::cast_to` never narrows under DF54 (#467).
        let unified = value_types
            .iter()
            .skip(1)
            .try_fold((*first).clone(), |acc, ty| {
                unify_graph_value_nullability(&acc, ty)
            });
        return DataType::new_list(unified.unwrap_or_else(|| (*first).clone()), true);
    }

    let mut variants = Vec::new();
    for arg_type in arg_types {
        let value_type = list_item_type(arg_type).unwrap_or(arg_type);
        if let DataType::Struct(fields) = value_type
            && is_dynamic_variant_struct(value_type)
        {
            for field in fields.iter().skip(1) {
                if !variants.contains(field.data_type()) {
                    variants.push(field.data_type().clone());
                }
            }
        } else if let DataType::Struct(fields) = value_type
            && matches!(
                graphforge_value::heterogeneous::recognize(value_type),
                Ok(Some(_))
            )
        {
            for (name, data_type) in [
                (graphforge_value::heterogeneous::INT, DataType::Int64),
                (graphforge_value::heterogeneous::FLOAT, DataType::Float64),
                (graphforge_value::heterogeneous::STR, DataType::Utf8),
                (graphforge_value::heterogeneous::BOOL, DataType::Boolean),
            ] {
                if fields.iter().any(|field| field.name() == name) && !variants.contains(&data_type)
                {
                    variants.push(data_type);
                }
            }
        } else if !matches!(value_type, DataType::Null) && !variants.contains(value_type) {
            variants.push(value_type.clone());
        }
    }
    if variants.is_empty() {
        variants.push(DataType::Null);
    }
    DataType::new_list(
        DataType::Struct(graphforge_value::heterogeneous::dynamic_fields(&variants)),
        true,
    )
}

fn list_plus_has_graph_value(arg_types: &[DataType]) -> bool {
    arg_types.iter().any(|arg_type| {
        let value_type = list_item_type(arg_type).unwrap_or(arg_type);
        dynamic_variant_types(value_type)
            .into_iter()
            .any(is_graph_value_struct)
    })
}

fn is_graph_value_struct(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(fields) if fields.iter().any(|field| {
        matches!(
            field.name().as_str(),
            "node_uuid" | "edge_uuid" | "nodes" | "relationships"
        )
    }))
}

fn is_dynamic_variant_struct(data_type: &DataType) -> bool {
    matches!(
        graphforge_value::heterogeneous::recognize(data_type),
        Ok(Some(
            graphforge_value::heterogeneous::Layout::DynamicV1 { .. }
        ))
    )
}

fn dynamic_variant_types(data_type: &DataType) -> Vec<&DataType> {
    if is_dynamic_variant_struct(data_type)
        && let DataType::Struct(fields) = data_type
    {
        return fields
            .iter()
            .skip(1)
            .map(|field| field.data_type())
            .collect();
    }
    vec![data_type]
}

fn list_item_type(dt: &DataType) -> Option<&DataType> {
    match dt {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            Some(f.data_type())
        }
        _ => None,
    }
}

fn list_plus_depth(arg_types: &[DataType]) -> usize {
    arg_types
        .iter()
        .filter_map(|data_type| {
            list_item_type(data_type)
                .or(Some(data_type))
                .and_then(het_depth_for_data_type)
        })
        .max()
        .unwrap_or(0)
}

fn het_depth_for_data_type(data_type: &DataType) -> Option<usize> {
    if is_het_struct_type(Some(data_type)) {
        return het_struct_type_depth(data_type);
    }
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8 => Some(0),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(1 + het_depth_for_data_type(field.data_type())?)
        }
        DataType::Struct(fields) if is_plain_map_struct_type(data_type) => fields
            .iter()
            .filter_map(|field| het_depth_for_data_type(field.data_type()))
            .max()
            .map_or(Some(1), |depth| Some(1 + depth)),
        _ => None,
    }
}

fn het_struct_type_depth(data_type: &DataType) -> Option<usize> {
    let DataType::Struct(fields) = data_type else {
        return None;
    };
    let Some(list_field) = fields
        .iter()
        .find(|field| field.name() == graphforge_value::heterogeneous::LIST)
    else {
        return Some(0);
    };
    match list_field.data_type() {
        DataType::List(inner) => het_struct_type_depth(inner.data_type()).map(|depth| depth + 1),
        _ => Some(0),
    }
}

fn list_elements_at(
    array: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> datafusion::error::Result<Option<Vec<ScalarValue>>> {
    use datafusion::arrow::array::{Array, FixedSizeListArray, LargeListArray, ListArray};

    let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        if list.is_null(row) {
            return Ok(None);
        }
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        if list.is_null(row) {
            return Ok(None);
        }
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        if list.is_null(row) {
            return Ok(None);
        }
        list.value(row)
    } else {
        return Ok(None);
    };

    (0..values.len())
        .map(|i| ScalarValue::try_from_array(&values, i).map(unwrap_het))
        .collect::<datafusion::error::Result<Vec<_>>>()
        .map(Some)
}

fn scalar_list_elements(
    value: &ScalarValue,
) -> datafusion::error::Result<Option<Vec<ScalarValue>>> {
    use datafusion::arrow::array::Array;

    match value {
        ScalarValue::List(list) => {
            if list.is_null(0) {
                return Ok(None);
            }
            let values = list.value(0);
            (0..values.len())
                .map(|i| ScalarValue::try_from_array(&values, i).map(unwrap_het))
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map(Some)
        }
        ScalarValue::LargeList(list) => {
            if list.is_null(0) {
                return Ok(None);
            }
            let values = list.value(0);
            (0..values.len())
                .map(|i| ScalarValue::try_from_array(&values, i).map(unwrap_het))
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map(Some)
        }
        _ => Ok(None),
    }
}

/// Const-fold an all-map list of DIFFERENT shapes into a homogeneous
/// `List<Struct<union-of-keys>>` literal — each map padded with a typed null for
/// keys it lacks (Cypher: a missing key reads as `null`), so `x.field` access
/// works and the list is a literal that can nest. `None` on an unresolvable key
/// type conflict (same key, two different non-null types) — left to `make_array`.
/// (#1005)
fn all_map_union_list(scalars: &[ScalarValue]) -> Option<DfExpr> {
    use datafusion::arrow::array::{Array, ArrayRef, StructArray, new_null_array};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::compute::{cast, concat};
    use datafusion::arrow::datatypes::{Field, Fields};
    use std::collections::HashMap;
    use std::sync::Arc;

    // Ordered union of key → resolved (non-null) type; a null-typed field yields
    // to a later real type, two differing real types are a conflict.
    let mut order: Vec<String> = Vec::new();
    let mut types: HashMap<String, DataType> = HashMap::new();
    for s in scalars {
        let arr = match s {
            ScalarValue::Struct(a) => a,
            ScalarValue::Null => continue,
            _ => return None,
        };
        for f in arr.fields() {
            let t = f.data_type().clone();
            match types.get(f.name()) {
                None => {
                    order.push(f.name().clone());
                    types.insert(f.name().clone(), t);
                }
                Some(prev) if *prev == DataType::Null => {
                    types.insert(f.name().clone(), t);
                }
                Some(prev) if t != DataType::Null && t != *prev => return None,
                _ => {}
            }
        }
    }
    let union_fields: Fields = order
        .iter()
        .map(|n| Field::new(n, types.get(n).cloned().unwrap_or(DataType::Null), true))
        .collect::<Vec<_>>()
        .into();

    // One concatenated column per union key (each row cast to the union type, a
    // missing key or a null-list-element → a typed null).
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(order.len());
    for name in &order {
        let ut = types.get(name).cloned().unwrap_or(DataType::Null);
        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(scalars.len());
        for s in scalars {
            let piece = match s {
                ScalarValue::Struct(a) => a
                    .column_by_name(name)
                    .and_then(|c| cast(c, &ut).ok())
                    .unwrap_or_else(|| new_null_array(&ut, 1)),
                _ => new_null_array(&ut, 1),
            };
            pieces.push(piece);
        }
        let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
        columns.push(concat(&refs).ok()?);
    }
    // A `null` list element (not a map) → a null struct row.
    let valid: NullBuffer = scalars.iter().map(|s| !s.is_null()).collect();
    let elem = StructArray::try_new(union_fields, columns, Some(valid)).ok()?;
    let n = scalars.len();
    let list_field = Arc::new(Field::new("item", elem.data_type().clone(), true));
    let list = datafusion::arrow::array::ListArray::new(
        list_field,
        OffsetBuffer::from_lengths([n]),
        Arc::new(elem),
        None,
    );
    Some(DfExpr::Literal(ScalarValue::List(Arc::new(list)), None))
}
