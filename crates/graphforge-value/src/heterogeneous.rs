//! Existing heterogeneous Arrow wire contracts, independent of DataFusion and I/O.
//!
//! Versions describe recognized layouts; they add no metadata to persisted values.
use std::sync::Arc;

use crate::Literal;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int8Array, Int64Array, ListArray, StringArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};

/// Persisted field name for tag.
pub const TAG: &str = "__het_tag";
/// Persisted field name for key.
pub const KEY: &str = "__het_key";
/// Persisted field name for int.
pub const INT: &str = "__het_int";
/// Persisted field name for float.
pub const FLOAT: &str = "__het_float";
/// Persisted field name for str.
pub const STR: &str = "__het_str";
/// Persisted field name for bool.
pub const BOOL: &str = "__het_bool";
/// Persisted field name for list.
pub const LIST: &str = "__het_list";
/// Persisted field name for map.
pub const MAP: &str = "__het_map";
/// Persisted field name for map key.
pub const MAP_KEY: &str = "__het_mkey";
/// Persisted field name for map value.
pub const MAP_VALUE: &str = "__het_mval";
/// Persisted field name for dynamic prefix.
pub const DYNAMIC_PREFIX: &str = "__het_value_";

/// Stable typed failures for supported heterogeneous value contracts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_SCHEMA: heterogeneous schema is not a supported layout")]
    Schema,
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_TAG: invalid heterogeneous tag {0}")]
    Tag(i8),
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_NULL: heterogeneous tag or selected payload is null")]
    NullPayload,
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_PAYLOAD: heterogeneous row has conflicting payloads")]
    ConflictingPayload,
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_BOUNDS: heterogeneous row or nesting exceeds its layout")]
    Bounds,
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_KIND: value cannot be encoded in this heterogeneous layout")]
    Kind,
    /// A wire contract invariant was violated.
    #[error("GF_VALUE_ARROW: {0}")]
    Arrow(String),
}

impl ValueError {
    /// Stable content-free category for adapters with sanitized diagnostics.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Schema => "GF_VALUE_SCHEMA",
            Self::Tag(_) => "GF_VALUE_TAG",
            Self::NullPayload => "GF_VALUE_NULL",
            Self::ConflictingPayload => "GF_VALUE_PAYLOAD",
            Self::Bounds => "GF_VALUE_BOUNDS",
            Self::Kind => "GF_VALUE_KIND",
            Self::Arrow(_) => "GF_VALUE_ARROW",
        }
    }
}

/// Descriptive versions of the three existing wire layouts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Layout {
    /// Persisted scalar properties: tag 4 means explicit null, no sort key.
    ScalarV1,
    /// Constant expressions: tag 4=list and 5=map; finite nesting depth.
    ConstantV1 {
        /// Finite list/map nesting depth.
        depth: usize,
    },
    /// Dynamic expressions: tags index payload fields, irrespective of kind.
    DynamicV1 {
        /// Payload data types in per-expression tag order.
        payload_types: Vec<DataType>,
    },
}

#[must_use]
/// Name of a dynamic payload at its per-expression index.
pub fn payload_field(index: impl std::fmt::Display) -> String {
    format!("{DYNAMIC_PREFIX}{index}")
}

#[must_use]
/// Canonical stored scalar fields, without an ordering key.
pub fn scalar_fields() -> Fields {
    vec![
        Field::new(TAG, DataType::Int8, false),
        Field::new(INT, DataType::Int64, true),
        Field::new(FLOAT, DataType::Float64, true),
        Field::new(STR, DataType::Utf8, true),
        Field::new(BOOL, DataType::Boolean, true),
    ]
    .into()
}

#[must_use]
/// Canonical key/value entry fields at the given value depth.
pub fn map_entry_fields(depth: usize) -> Fields {
    vec![
        Field::new(MAP_KEY, DataType::Utf8, false),
        Field::new(MAP_VALUE, DataType::Struct(constant_fields(depth)), true),
    ]
    .into()
}

#[must_use]
/// Canonical constant-expression fields, including numeric ordering key.
pub fn constant_fields(depth: usize) -> Fields {
    let mut fields = vec![Field::new(KEY, DataType::Float64, true)];
    fields.extend(scalar_fields().iter().map(|field| field.as_ref().clone()));
    if depth > 0 {
        fields.push(Field::new(
            LIST,
            DataType::new_list(DataType::Struct(constant_fields(depth - 1)), true),
            true,
        ));
        fields.push(Field::new(
            MAP,
            DataType::new_list(DataType::Struct(map_entry_fields(depth - 1)), true),
            true,
        ));
    }
    fields.into()
}

/// Canonical per-expression payload-index fields.
#[must_use]
pub fn dynamic_fields(types: &[DataType]) -> Fields {
    let mut fields = vec![Field::new(TAG, DataType::Int8, false)];
    fields.extend(
        types
            .iter()
            .enumerate()
            .map(|(i, ty)| Field::new(payload_field(i), ty.clone(), true)),
    );
    fields.into()
}

/// Recognize ordinary structs separately from malformed reserved heterogeneous schemas.
///
/// # Errors
/// Returns a schema error when a tagged struct does not match an existing layout.
pub fn recognize(data_type: &DataType) -> Result<Option<Layout>, ValueError> {
    let DataType::Struct(fields) = data_type else {
        return Ok(None);
    };
    // Only the existing tag marker identifies this contract. Payload-like
    // names alone remain valid ordinary map/property names.
    let Some(tag) = fields.iter().find(|field| field.name() == TAG) else {
        return Ok(None);
    };
    let names = fields
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let scalar_names = [TAG, INT, FLOAT, STR, BOOL];
    let constant_names = [KEY, TAG, INT, FLOAT, STR, BOOL];
    let complete_names = names == scalar_names
        || names == constant_names
        || names == [KEY, TAG, INT, FLOAT, STR, BOOL, LIST, MAP]
        || (names.first() == Some(&TAG)
            && names.len() > 1
            && names
                .iter()
                .skip(1)
                .enumerate()
                .all(|(index, name)| *name == payload_field(index)));
    // Cypher maps and node properties may use the tag name as ordinary data.
    // A full wire footprint still rejects tag-type drift rather than becoming
    // an ordinary map merely because its tag column has the wrong type.
    if names.len() == 1 || (tag.data_type() != &DataType::Int8 && !complete_names) {
        return Ok(None);
    }
    if *fields == scalar_fields() {
        return Ok(Some(Layout::ScalarV1));
    }
    if fields.first().is_some_and(|field| field.name() == KEY) {
        let depth = match fields.iter().find(|field| field.name() == LIST) {
            None => 0,
            Some(field) => {
                let DataType::List(item) = field.data_type() else {
                    return Err(ValueError::Schema);
                };
                let Some(Layout::ConstantV1 { depth }) = recognize(item.data_type())? else {
                    return Err(ValueError::Schema);
                };
                depth.checked_add(1).ok_or(ValueError::Bounds)?
            }
        };
        if *fields != constant_fields(depth) {
            return Err(ValueError::Schema);
        }
        return Ok(Some(Layout::ConstantV1 { depth }));
    }
    let types: Vec<_> = fields
        .iter()
        .skip(1)
        .map(|field| field.data_type().clone())
        .collect();
    if i8::try_from(types.len()).is_ok() && *fields == dynamic_fields(&types) {
        return Ok(Some(Layout::DynamicV1 {
            payload_types: types,
        }));
    }
    Err(ValueError::Schema)
}

/// Selected payload stays borrowed; callers perform only their own logical-type conversion.
pub enum Decoded<'a> {
    /// A null struct row or explicit stored null.
    Null,
    /// Selected payload in its original Arrow type.
    Payload(&'a ArrayRef),
    /// Constant-layout map entries requiring logical map adaptation.
    Map(&'a ArrayRef),
}

fn payload_is_null(array: &dyn Array, row: usize) -> bool {
    if array.data_type() == &DataType::Null || array.is_null(row) {
        return true;
    }
    // Encoded arrays can point at a null value without a null key. Slice first
    // so logical validity evaluation remains bounded to this one selected row.
    matches!(
        array.data_type(),
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _)
    ) && array.slice(row, 1).logical_null_count() == 1
}

/// Validate and select a single heterogeneous row without copying its payload.
///
/// # Errors
/// Returns a typed schema, tag, payload, or bounds error for an invalid row.
pub fn decode_row(array: &StructArray, row: usize) -> Result<Decoded<'_>, ValueError> {
    if row >= array.len() {
        return Err(ValueError::Bounds);
    }
    let layout = recognize(array.data_type())?.ok_or(ValueError::Schema)?;
    if array.is_null(row) {
        return Ok(Decoded::Null);
    }
    let tags = array
        .column_by_name(TAG)
        .and_then(|a| a.as_any().downcast_ref::<Int8Array>())
        .ok_or(ValueError::Schema)?;
    if tags.is_null(row) {
        return Err(ValueError::NullPayload);
    }
    let tag = tags.value(row);
    let (selected, map, first_payload) = match layout {
        Layout::ScalarV1 => match tag {
            0..=3 => (
                Some(usize::try_from(tag).map_err(|_| ValueError::Tag(tag))? + 1),
                false,
                1,
            ),
            4 => (None, false, 1),
            _ => return Err(ValueError::Tag(tag)),
        },
        Layout::ConstantV1 { depth } => match tag {
            0..=3 => (
                Some(usize::try_from(tag).map_err(|_| ValueError::Tag(tag))? + 2),
                false,
                2,
            ),
            4..=5 if depth > 0 => (
                Some(usize::try_from(tag).map_err(|_| ValueError::Tag(tag))? + 2),
                tag == 5,
                2,
            ),
            _ => return Err(ValueError::Tag(tag)),
        },
        Layout::DynamicV1 { ref payload_types } => {
            let index = usize::try_from(tag).map_err(|_| ValueError::Tag(tag))?;
            if index >= payload_types.len() {
                return Err(ValueError::Tag(tag));
            }
            (Some(index + 1), false, 1)
        }
    };
    for index in first_payload..array.num_columns() {
        if Some(index) != selected && !payload_is_null(array.column(index).as_ref(), row) {
            return Err(ValueError::ConflictingPayload);
        }
    }
    let Some(index) = selected else {
        return Ok(Decoded::Null);
    };
    let payload = array.column(index);
    if payload_is_null(payload.as_ref(), row) {
        return Err(ValueError::NullPayload);
    }
    Ok(if map {
        Decoded::Map(payload)
    } else {
        Decoded::Payload(payload)
    })
}

/// Check nested schemas and identify whether value validation is necessary.
///
/// # Errors
/// Returns a typed schema error for malformed reserved heterogeneous layouts.
///
/// # Errors
/// Returns a schema error for a malformed heterogeneous type at any nesting depth.
pub fn contains_heterogeneous(data_type: &DataType) -> Result<bool, ValueError> {
    let own = recognize(data_type)?.is_some();
    let mut nested = false;
    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                nested |= contains_heterogeneous(field.data_type())?;
            }
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => nested = contains_heterogeneous(field.data_type())?,
        DataType::Dictionary(_, value) => nested = contains_heterogeneous(value)?,
        _ => {}
    }
    Ok(own || nested)
}

/// Validate nested values before exposing a batch, including dynamic nested structs.
///
/// # Errors
/// Returns a typed error for malformed visible values or nested schemas.
pub fn validate_array(array: &dyn Array) -> Result<(), ValueError> {
    use arrow::array::{FixedSizeListArray, LargeListArray, MapArray};
    if !contains_heterogeneous(array.data_type())? {
        return Ok(());
    }
    if let DataType::Dictionary(_, value) = array.data_type() {
        let decoded = arrow::compute::cast(array, value)
            .map_err(|error| ValueError::Arrow(error.to_string()))?;
        return validate_array(decoded.as_ref());
    }
    if let Some(values) = array.as_any().downcast_ref::<StructArray>() {
        if recognize(values.data_type())?.is_some() {
            for row in 0..values.len() {
                match decode_row(values, row)? {
                    Decoded::Null => {}
                    Decoded::Payload(value) | Decoded::Map(value) => {
                        validate_array(value.slice(row, 1).as_ref())?;
                    }
                }
            }
        } else {
            for row in 0..values.len() {
                if !values.is_null(row) {
                    for child in values.columns() {
                        validate_array(child.slice(row, 1).as_ref())?;
                    }
                }
            }
        }
    } else if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
        for row in 0..values.len() {
            if !values.is_null(row) {
                validate_array(values.value(row).as_ref())?;
            }
        }
    } else if let Some(values) = array.as_any().downcast_ref::<LargeListArray>() {
        for row in 0..values.len() {
            if !values.is_null(row) {
                validate_array(values.value(row).as_ref())?;
            }
        }
    } else if let Some(values) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        for row in 0..values.len() {
            if !values.is_null(row) {
                validate_array(values.value(row).as_ref())?;
            }
        }
    } else if let Some(values) = array.as_any().downcast_ref::<MapArray>() {
        for row in 0..values.len() {
            if !values.is_null(row) {
                validate_array(&values.value(row))?;
            }
        }
    }
    Ok(())
}

/// Encode the existing constant/nested layout from neutral values.
///
/// # Errors
/// Rejects unsupported literal kinds or insufficient nesting depth.
#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive wire-kind table keeps payload columns and validity aligned"
)]
pub fn encode_constant(values: &[Literal], depth: usize) -> Result<StructArray, ValueError> {
    let mut keys = Vec::new();
    let mut tags = Vec::new();
    let mut ints = Vec::new();
    let mut floats = Vec::new();
    let mut strings = Vec::new();
    let mut bools = Vec::new();
    let mut valid = Vec::new();
    let mut children = Vec::new();
    let mut offsets = vec![0_i32];
    let mut child_valid = Vec::new();
    let mut map_keys = Vec::new();
    let mut map_values = Vec::new();
    let mut map_offsets = vec![0_i32];
    let mut map_valid = Vec::new();
    for value in values {
        let (mut key, mut tag, mut int, mut float, mut string, mut boolean) =
            (None, 0, None, None, None, None);
        match value {
            Literal::Null => {}
            Literal::Int(v) => {
                #[allow(clippy::cast_precision_loss)]
                {
                    key = Some(*v as f64);
                }
                int = Some(*v);
            }
            Literal::Float(v) => {
                key = Some(*v);
                float = Some(*v);
                tag = 1;
            }
            Literal::Str(v) => {
                string = Some(v.clone());
                tag = 2;
            }
            Literal::Bool(v) => {
                boolean = Some(*v);
                tag = 3;
            }
            Literal::List(v) if depth > 0 => {
                children.extend(v.iter().cloned());
                tag = 4;
            }
            Literal::Map(v) if depth > 0 => {
                for (k, v) in v {
                    map_keys.push(k.clone());
                    map_values.push(v.clone());
                }
                tag = 5;
            }
            _ => return Err(ValueError::Kind),
        }
        keys.push(key);
        tags.push(tag);
        ints.push(int);
        floats.push(float);
        strings.push(string);
        bools.push(boolean);
        valid.push(!matches!(value, Literal::Null));
        child_valid.push(tag == 4);
        map_valid.push(tag == 5);
        offsets.push(i32::try_from(children.len()).map_err(|_| ValueError::Bounds)?);
        map_offsets.push(i32::try_from(map_keys.len()).map_err(|_| ValueError::Bounds)?);
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(keys)),
        Arc::new(Int8Array::from(tags)),
        Arc::new(Int64Array::from(ints)),
        Arc::new(Float64Array::from(floats)),
        Arc::new(StringArray::from(strings)),
        Arc::new(BooleanArray::from(bools)),
    ];
    if depth > 0 {
        columns.push(Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(constant_fields(depth - 1)),
                true,
            )),
            OffsetBuffer::new(offsets.into()),
            Arc::new(encode_constant(&children, depth - 1)?),
            Some(NullBuffer::from(child_valid)),
        )));
        let entries = StructArray::new(
            map_entry_fields(depth - 1),
            vec![
                Arc::new(StringArray::from(map_keys)),
                Arc::new(encode_constant(&map_values, depth - 1)?),
            ],
            None,
        );
        columns.push(Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(map_entry_fields(depth - 1)),
                true,
            )),
            OffsetBuffer::new(map_offsets.into()),
            Arc::new(entries),
            Some(NullBuffer::from(map_valid)),
        )));
    }
    StructArray::try_new(
        constant_fields(depth),
        columns,
        Some(NullBuffer::from(valid)),
    )
    .map_err(|e| ValueError::Arrow(e.to_string()))
}

/// Only values representable by the persisted scalar layout can enter its encoder.
#[derive(Clone, Copy)]
pub enum Scalar<'a> {
    /// Explicit null, distinct from an absent property.
    Null,
    /// Signed integer.
    Int(i64),
    /// Floating point number, including non-finite values.
    Float(f64),
    /// UTF-8 string.
    Str(&'a str),
    /// Boolean.
    Bool(bool),
}

#[must_use]
/// Encode canonical stored scalar properties, distinguishing absent and explicit null.
pub fn encode_scalar<'a>(values: impl IntoIterator<Item = Option<Scalar<'a>>>) -> StructArray {
    let mut tags = Vec::new();
    let mut ints = Vec::new();
    let mut floats = Vec::new();
    let mut strings = Vec::new();
    let mut bools = Vec::new();
    let mut valid = Vec::new();
    for value in values {
        let (tag, int, float, string, boolean) = match value {
            Some(Scalar::Int(v)) => (0, Some(v), None, None, None),
            Some(Scalar::Float(v)) => (1, None, Some(v), None, None),
            Some(Scalar::Str(v)) => (2, None, None, Some(v), None),
            Some(Scalar::Bool(v)) => (3, None, None, None, Some(v)),
            Some(Scalar::Null) => (4, None, None, None, None),
            None => (0, None, None, None, None),
        };
        tags.push(tag);
        ints.push(int);
        floats.push(float);
        strings.push(string);
        bools.push(boolean);
        valid.push(value.is_some());
    }
    StructArray::new(
        scalar_fields(),
        vec![
            Arc::new(Int8Array::from(tags)),
            Arc::new(Int64Array::from(ints)),
            Arc::new(Float64Array::from(floats)),
            Arc::new(StringArray::from(strings)),
            Arc::new(BooleanArray::from(bools)),
        ],
        Some(NullBuffer::from(valid)),
    )
}

/// Decode one canonical stored scalar property.
///
/// # Errors
/// Rejects non-scalar layouts and malformed selected values.
pub fn decode_scalar(array: &StructArray, row: usize) -> Result<Literal, ValueError> {
    if recognize(array.data_type())? != Some(Layout::ScalarV1) {
        return Err(ValueError::Schema);
    }
    match decode_row(array, row)? {
        Decoded::Null => Ok(Literal::Null),
        Decoded::Payload(value) => match value.data_type() {
            DataType::Int64 => Ok(Literal::Int(
                value
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or(ValueError::Schema)?
                    .value(row),
            )),
            DataType::Float64 => Ok(Literal::Float(
                value
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or(ValueError::Schema)?
                    .value(row),
            )),
            DataType::Utf8 => Ok(Literal::Str(
                value
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(ValueError::Schema)?
                    .value(row)
                    .to_owned(),
            )),
            DataType::Boolean => Ok(Literal::Bool(
                value
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or(ValueError::Schema)?
                    .value(row),
            )),
            _ => Err(ValueError::Schema),
        },
        Decoded::Map(_) => Err(ValueError::Schema),
    }
}

/// Assemble existing dynamic rows from selected payload indexes and typed columns.
///
/// # Errors
/// Rejects mismatched lengths, invalid indexes, and conflicting or null payloads.
pub fn encode_dynamic_rows(
    tags: Int8Array,
    payloads: Vec<ArrayRef>,
    validity: Option<NullBuffer>,
) -> Result<StructArray, ValueError> {
    if payloads.len() > i8::MAX as usize {
        return Err(ValueError::Bounds);
    }
    let types = payloads
        .iter()
        .map(|value| value.data_type().clone())
        .collect::<Vec<_>>();
    let mut columns = vec![Arc::new(tags) as ArrayRef];
    columns.extend(payloads);
    let result = StructArray::try_new(dynamic_fields(&types), columns, validity)
        .map_err(|error| ValueError::Arrow(error.to_string()))?;
    validate_array(&result)?;
    Ok(result)
}

/// Encode one dynamic expression list per input row, keeping tags as payload indexes.
///
/// # Errors
/// Rejects mismatched lengths, excessive width, invalid values, or offset overflow.
pub fn encode_dynamic(values: &[ArrayRef], rows: usize) -> Result<ListArray, ValueError> {
    use arrow::array::Int32Array;
    use arrow::compute::take;
    let width = values.len();
    let width_i8 = i8::try_from(width).map_err(|_| ValueError::Bounds)?;
    if values.iter().any(|value| value.len() != rows) {
        return Err(ValueError::Bounds);
    }
    rows.checked_mul(width)
        .filter(|length| i32::try_from(*length).is_ok())
        .ok_or(ValueError::Bounds)?;
    let types = values
        .iter()
        .map(|value| value.data_type().clone())
        .collect::<Vec<_>>();
    let fields = dynamic_fields(&types);
    let tags = Int8Array::from_iter_values((0..rows).flat_map(|_| 0..width_i8));
    let mut columns: Vec<ArrayRef> = vec![Arc::new(tags)];
    for (index, value) in values.iter().enumerate() {
        let indices = (0..rows)
            .flat_map(|row| {
                (0..width).map(move |element| {
                    (element == index)
                        .then(|| i32::try_from(row).ok())
                        .flatten()
                })
            })
            .collect::<Int32Array>();
        columns.push(
            take(value.as_ref(), &indices, None).map_err(|e| ValueError::Arrow(e.to_string()))?,
        );
    }
    let validity = (0..rows)
        .flat_map(|row| {
            values
                .iter()
                .map(move |value| !payload_is_null(value.as_ref(), row))
        })
        .collect::<NullBuffer>();
    let tags = columns.remove(0);
    let tags = tags
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or(ValueError::Schema)?
        .clone();
    let elements = encode_dynamic_rows(tags, columns, Some(validity))?;
    Ok(ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        Arc::new(elements),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_v1_golden_fields_tags_and_null_states() {
        let expected: Fields = vec![
            Field::new("__het_tag", DataType::Int8, false),
            Field::new("__het_int", DataType::Int64, true),
            Field::new("__het_float", DataType::Float64, true),
            Field::new("__het_str", DataType::Utf8, true),
            Field::new("__het_bool", DataType::Boolean, true),
        ]
        .into();
        assert_eq!(scalar_fields(), expected);
        let values = encode_scalar([
            Some(Scalar::Int(i64::MAX)),
            Some(Scalar::Float(-1.5)),
            Some(Scalar::Str("text")),
            Some(Scalar::Bool(true)),
            Some(Scalar::Null),
            None,
        ]);
        let tags = values
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(tags.values().as_ref(), &[0, 1, 2, 3, 4, 0]);
        assert!(!values.is_null(4));
        assert!(values.is_null(5));
        let expected = [
            Literal::Int(i64::MAX),
            Literal::Float(-1.5),
            Literal::Str("text".into()),
            Literal::Bool(true),
            Literal::Null,
            Literal::Null,
        ];
        for (row, value) in expected.iter().enumerate() {
            assert_eq!(&decode_scalar(&values, row).unwrap(), value);
        }
        validate_array(&values).unwrap();
    }

    #[test]
    fn constant_v1_golden_nested_tags_and_dynamic_index_are_distinct() {
        let values = encode_constant(
            &[
                Literal::Int(1),
                Literal::Float(2.5),
                Literal::Str("x".into()),
                Literal::Bool(false),
                Literal::List(vec![Literal::Int(9)]),
                Literal::Map(vec![("key".into(), Literal::Null)]),
                Literal::Null,
            ],
            1,
        )
        .unwrap();
        assert_eq!(
            values
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            [
                "__het_key",
                "__het_tag",
                "__het_int",
                "__het_float",
                "__het_str",
                "__het_bool",
                "__het_list",
                "__het_map"
            ]
        );
        let tags = values
            .column(1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(tags.values().as_ref(), &[0, 1, 2, 3, 4, 5, 0]);
        assert!(
            matches!(decode_row(&values, 4).unwrap(), Decoded::Payload(value) if matches!(value.data_type(), DataType::List(_)))
        );
        assert!(matches!(decode_row(&values, 5).unwrap(), Decoded::Map(_)));
        validate_array(&values).unwrap();
        let columns: Vec<ArrayRef> = (0..6)
            .map(|i| Arc::new(Int64Array::from(vec![i])) as ArrayRef)
            .collect();
        let dynamic = encode_dynamic(&columns, 1).unwrap();
        let items = dynamic.value(0);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(items.fields()[5].name(), "__het_value_4");
        assert!(
            matches!(decode_row(items, 4).unwrap(), Decoded::Payload(value) if value.data_type() == &DataType::Int64)
        );
        validate_array(&dynamic).unwrap();
    }

    #[test]
    fn dynamic_null_payload_is_logically_null_without_a_physical_bitmap() {
        let values: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["text"])),
            Arc::new(arrow::array::NullArray::new(1)),
            Arc::new(Int64Array::from(vec![7])),
        ];
        let lists = encode_dynamic(&values, 1).unwrap();
        validate_array(&lists).unwrap();
        let items = lists.value(0);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(matches!(decode_row(items, 0).unwrap(), Decoded::Payload(_)));
        assert!(matches!(decode_row(items, 1).unwrap(), Decoded::Null));
        assert!(matches!(decode_row(items, 2).unwrap(), Decoded::Payload(_)));
    }

    #[test]
    fn malformed_tags_payloads_and_schema_have_distinct_errors() {
        let original = encode_scalar([Some(Scalar::Int(3))]);
        let mut columns = original.columns().to_vec();
        columns[0] = Arc::new(Int8Array::from(vec![99]));
        let invalid = StructArray::new(scalar_fields(), columns.clone(), None);
        assert!(matches!(decode_row(&invalid, 0), Err(ValueError::Tag(99))));
        columns[0] = Arc::new(Int8Array::from(vec![0]));
        columns[1] = Arc::new(Int64Array::from(vec![None]));
        assert!(matches!(
            decode_row(&StructArray::new(scalar_fields(), columns.clone(), None), 0),
            Err(ValueError::NullPayload)
        ));
        columns[1] = Arc::new(Int64Array::from(vec![3]));
        columns[2] = Arc::new(Float64Array::from(vec![1.0]));
        assert!(matches!(
            decode_row(&StructArray::new(scalar_fields(), columns, None), 0),
            Err(ValueError::ConflictingPayload)
        ));
        let mut fields = scalar_fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields[1] = Field::new("__het_int", DataType::UInt64, true);
        assert_eq!(
            recognize(&DataType::Struct(fields.into())),
            Err(ValueError::Schema)
        );
        let mut fields = scalar_fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[0] = Field::new(TAG, DataType::Int64, false);
        assert_eq!(
            recognize(&DataType::Struct(fields.into())),
            Err(ValueError::Schema)
        );
    }

    #[test]
    fn ordinary_property_names_do_not_claim_a_heterogeneous_layout() {
        for name in ["__het_int", "__het_key", "__het_value_0", "__het_tag"] {
            let ordinary = StructArray::new(
                vec![Field::new(name, DataType::Int64, false)].into(),
                vec![Arc::new(Int64Array::from(vec![7]))],
                None,
            );
            assert_eq!(recognize(ordinary.data_type()), Ok(None));
            validate_array(&ordinary).unwrap();
        }
    }

    #[test]
    fn nested_validation_respects_null_parent_masks() {
        let original = encode_scalar([Some(Scalar::Int(3))]);
        let mut columns = original.columns().to_vec();
        columns[0] = Arc::new(Int8Array::from(vec![99]));
        let invalid = Arc::new(StructArray::new(scalar_fields(), columns, None)) as ArrayRef;
        let field = Arc::new(Field::new("item", invalid.data_type().clone(), true));
        let masked = ListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0, 1].into()),
            invalid.clone(),
            Some(NullBuffer::from(vec![false])),
        );
        validate_array(&masked).unwrap();
        let ordinary_fields =
            vec![Field::new("property", invalid.data_type().clone(), true)].into();
        let masked_struct = StructArray::new(
            ordinary_fields,
            vec![invalid.clone()],
            Some(NullBuffer::from(vec![false])),
        );
        validate_array(&masked_struct).unwrap();
        let masked_large = arrow::array::LargeListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0_i64, 1].into()),
            invalid.clone(),
            Some(NullBuffer::from(vec![false])),
        );
        validate_array(&masked_large).unwrap();
        let masked_fixed = arrow::array::FixedSizeListArray::new(
            field.clone(),
            1,
            invalid.clone(),
            Some(NullBuffer::from(vec![false])),
        );
        validate_array(&masked_fixed).unwrap();
        let unused_values = ListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0, 0].into()),
            invalid.clone(),
            None,
        );
        validate_array(&unused_values).unwrap();
        let visible = ListArray::new(field, OffsetBuffer::new(vec![0, 1].into()), invalid, None);
        assert_eq!(validate_array(&visible), Err(ValueError::Tag(99)));
    }
}
