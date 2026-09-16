//! Map/property access, map keys, and list subscripting execution.
//!
//! Expression lowering stays in the parent; this module owns access UDFs,
//! return-type inference, and key/index validation.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;

use super::value_semantics::{ListView, decode_het, validate_heterogeneous_arguments};
use super::{
    build_het_struct, const_map_scalar, het_fields, is_het_struct_type, is_plain_map_struct_type,
    unwrap_het,
};

// ---------------------------------------------------------------------------
// cypher_map_keys UDF
// ---------------------------------------------------------------------------

const ENTITY_PROPERTY_MAP_HET_DEPTH: usize = 3;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherEntityProperties {
    signature: Signature,
}

impl CypherEntityProperties {
    pub(super) fn new(arity: usize) -> Self {
        Self {
            signature: Signature::any(arity, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherEntityProperties {
    fn name(&self) -> &'static str {
        "cypher_entity_properties"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(het_fields(ENTITY_PROPERTY_MAP_HET_DEPTH)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::ArrayRef;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        if args.args.is_empty() || !(args.args.len() - 1).is_multiple_of(2) {
            return Err(DataFusionError::Plan(
                "properties() entity map expects present plus key/value pairs".into(),
            ));
        }
        let cols: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<datafusion::error::Result<_>>()?;
        let mut maps = Vec::with_capacity(rows);
        for row in 0..rows {
            let present = match ScalarValue::try_from_array(&cols[0], row)? {
                ScalarValue::Boolean(Some(true)) => true,
                ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => false,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "properties() entity presence must be boolean, got {other:?}"
                    )));
                }
            };
            if !present {
                maps.push(ScalarValue::Null);
                continue;
            }
            let mut entries = Vec::with_capacity((cols.len() - 1) / 2);
            for pair in cols[1..].chunks_exact(2) {
                let key = ScalarValue::try_from_array(&pair[0], row)?;
                let Some(key) = scalar_access_key(&key)? else {
                    continue;
                };
                let value = ScalarValue::try_from_array(&pair[1], row)?;
                if value.is_null() {
                    continue;
                }
                entries.push((key, unwrap_het(value)));
            }
            let map = const_map_scalar(&entries).ok_or_else(|| {
                DataFusionError::Execution(
                    "properties() could not encode entity property map".into(),
                )
            })?;
            maps.push(map);
        }
        let out = build_het_struct(&maps, ENTITY_PROPERTY_MAP_HET_DEPTH).ok_or_else(|| {
            DataFusionError::Execution("properties() could not encode entity property map".into())
        })?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

pub(super) static CYPHER_MAP_KEYS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherMapKeys::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherMapKeys {
    signature: Signature,
}

impl CypherMapKeys {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherMapKeys {
    fn name(&self) -> &'static str {
        "cypher_map_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(DataType::Utf8, true))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray, StructArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        if matches!(values.data_type(), DataType::Null) {
            let nulls = NullBuffer::from(vec![false; rows]);
            let list = ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; rows + 1])),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())) as ArrayRef,
                Some(nulls),
            );
            return Ok(ColumnarValue::Array(Arc::new(list)));
        }
        let DataType::Struct(fields) = values.data_type() else {
            return Err(DataFusionError::Execution(format!(
                "keys() requires a map, node, relationship, or null, got {:?}",
                values.data_type()
            )));
        };
        let map = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("keys() expected a struct map".into()))?;
        if is_het_struct_type(Some(values.data_type())) {
            return tagged_map_keys(map, rows);
        }
        if !is_plain_map_struct_type(values.data_type()) {
            return Err(DataFusionError::Execution(format!(
                "keys() requires a map, node, relationship, or null, got {:?}",
                values.data_type()
            )));
        }
        let names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        let mut offsets = Vec::with_capacity(rows + 1);
        let mut values = Vec::new();
        let mut valid = Vec::with_capacity(rows);
        offsets.push(0_i32);
        for row in 0..rows {
            if map.is_null(row) {
                valid.push(false);
            } else {
                valid.push(true);
                values.extend(names.iter().cloned().map(Some));
            }
            offsets.push(i32::try_from(values.len()).map_err(|_| {
                DataFusionError::Execution("keys() result exceeded i32 list offsets".into())
            })?);
        }
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(StringArray::from(values)) as ArrayRef,
            Some(NullBuffer::from(valid)),
        );
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

fn tagged_map_keys(
    map: &datafusion::arrow::array::StructArray,
    rows: usize,
) -> datafusion::error::Result<ColumnarValue> {
    use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray, StructArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    let entries = map
        .column_by_name(graphforge_value::heterogeneous::MAP)
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| DataFusionError::Plan("tagged map is missing __het_map".into()))?;
    let mut offsets = Vec::with_capacity(rows + 1);
    let mut values = Vec::new();
    let mut valid = Vec::with_capacity(rows);
    offsets.push(0_i32);
    for row in 0..rows {
        if map.is_null(row) {
            valid.push(false);
        } else {
            if !matches!(
                graphforge_value::heterogeneous::decode_row(map, row)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
                graphforge_value::heterogeneous::Decoded::Map(_)
            ) {
                return Err(DataFusionError::Execution(
                    "keys() requires a map, node, relationship, or null".into(),
                ));
            }
            valid.push(true);
            if !entries.is_null(row) {
                let entry_values = entries.value(row);
                let entry_struct = entry_values
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("tagged map entries must be structs".into())
                    })?;
                let map_keys = entry_struct
                    .column_by_name(graphforge_value::heterogeneous::MAP_KEY)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| {
                        DataFusionError::Plan("tagged map entries must carry __het_mkey".into())
                    })?;
                for idx in 0..entry_struct.len() {
                    if !map_keys.is_null(idx) {
                        values.push(Some(map_keys.value(idx).to_owned()));
                    }
                }
            }
        }
        offsets.push(i32::try_from(values.len()).map_err(|_| {
            DataFusionError::Execution("keys() result exceeded i32 list offsets".into())
        })?);
    }
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(StringArray::from(values)) as ArrayRef,
        Some(NullBuffer::from(valid)),
    );
    Ok(ColumnarValue::Array(Arc::new(list)))
}

// ---------------------------------------------------------------------------
// cypher_value_access UDF
// ---------------------------------------------------------------------------

pub(super) static CYPHER_VALUE_ACCESS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherValueAccess::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherStaticValueAccess {
    key: String,
    signature: Signature,
}

impl CypherStaticValueAccess {
    pub(super) fn new(key: String) -> Self {
        Self {
            key,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherStaticValueAccess {
    fn name(&self) -> &'static str {
        "cypher_static_value_access"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        static_value_access_return_type(arg_types.first(), &self.key)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            static_value_access_return_type(
                args.arg_fields.first().map(|field| field.data_type()),
                &self.key,
            )?,
            true,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let return_type = args.return_field.data_type();
        let null_value = ScalarValue::try_new_null(return_type)?;
        let output = (0..rows)
            .map(|row| {
                let value = ScalarValue::try_from_array(&values, row)?;
                let value = decode_het(&value).unwrap_or(value);
                if value.is_null() {
                    return Ok(null_value.clone());
                }
                let ScalarValue::Struct(value) = value else {
                    return Err(DataFusionError::Execution(
                        "InvalidArgumentValue: property access requires a map or graph element"
                            .into(),
                    ));
                };
                let Some(column) = value.column_by_name(&self.key) else {
                    return Ok(null_value.clone());
                };
                let result = ScalarValue::try_from_array(column, 0)?;
                if result.data_type() == *return_type {
                    Ok(result)
                } else if result.is_null() {
                    Ok(null_value.clone())
                } else {
                    Err(DataFusionError::Execution(format!(
                        "property `{}` has incompatible runtime type {:?}; expected {:?}",
                        self.key,
                        result.data_type(),
                        return_type
                    )))
                }
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(output)?))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherValueAccess {
    signature: Signature,
}

impl CypherValueAccess {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherValueAccess {
    fn name(&self) -> &'static str {
        "cypher_value_access"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        value_access_return_type(arg_types.first())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        Ok(std::sync::Arc::new(Field::new(
            self.name(),
            value_access_return_type(args.arg_fields.first().map(|f| f.data_type()))?,
            true,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::StructArray;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let keys = args.args[1].to_array(rows)?;
        let return_type = args.return_field.data_type().clone();
        let null_value = ScalarValue::try_from(&return_type).unwrap_or(ScalarValue::Null);
        if matches!(values.data_type(), DataType::Null) {
            return Ok(ColumnarValue::Array(ScalarValue::iter_to_array(
                (0..rows).map(|_| null_value.clone()),
            )?));
        }
        if let Some(list) = ListView::from_array(&values) {
            let out = (0..rows)
                .map(|i| list_access_value(&list, &keys, i, &null_value))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            return Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?));
        }
        let map = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "dynamic subscript requires a list or map/entity struct, got {:?}",
                    values.data_type()
                ))
            })?;
        if is_het_struct_type(Some(values.data_type())) {
            let out = (0..rows)
                .map(|i| het_map_access_value(map, &keys, i, &null_value))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            return Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?));
        }
        let out = (0..rows)
            .map(|i| {
                if map.is_null(i) {
                    return Ok(null_value.clone());
                }
                let key = ScalarValue::try_from_array(&keys, i)?;
                let Some(key) = scalar_access_key(&key)? else {
                    return Ok(null_value.clone());
                };
                let Some(col) = map.column_by_name(&key) else {
                    return Ok(null_value.clone());
                };
                ScalarValue::try_from_array(col, i)
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?))
    }
}

fn value_access_return_type(dt: Option<&DataType>) -> datafusion::error::Result<DataType> {
    let Some(dt) = dt else {
        return Ok(DataType::Null);
    };
    match dt {
        DataType::Null => Ok(DataType::Null),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Ok(field.data_type().clone())
        }
        dt if is_het_struct_type(Some(dt)) => het_value_access_return_type(dt),
        DataType::Struct(fields) => common_struct_field_type(fields),
        // Parameter values are bound after logical lowering. If a parameter later
        // turns out not to be a list/map/entity, defer the invalid-argument failure to
        // UDF invocation so Cypher observes it as a runtime type error.
        _ => Ok(DataType::Null),
    }
}

fn static_value_access_return_type(
    dt: Option<&DataType>,
    key: &str,
) -> datafusion::error::Result<DataType> {
    let Some(DataType::Struct(fields)) = dt else {
        return Ok(DataType::Null);
    };
    if !is_het_struct_type(dt) {
        return fields
            .iter()
            .find(|field| field.name() == key)
            .map_or(Ok(DataType::Null), |field| Ok(field.data_type().clone()));
    }
    if fields
        .iter()
        .any(|field| field.name() == graphforge_value::heterogeneous::MAP)
    {
        return het_value_access_return_type(&DataType::Struct(fields.clone()));
    }
    let mut data_type = None;
    for variant in fields.iter().filter(|field| {
        field
            .name()
            .starts_with(graphforge_value::heterogeneous::DYNAMIC_PREFIX)
    }) {
        let DataType::Struct(value_fields) = variant.data_type() else {
            continue;
        };
        let Some(property) = value_fields.iter().find(|field| field.name() == key) else {
            continue;
        };
        if matches!(property.data_type(), DataType::Null) {
            continue;
        }
        match &data_type {
            None => data_type = Some(property.data_type().clone()),
            Some(existing) if existing == property.data_type() => {}
            Some(existing) => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "property `{key}` has incompatible graph-value types {existing:?} and {:?}",
                    property.data_type()
                )));
            }
        }
    }
    Ok(data_type.unwrap_or(DataType::Null))
}

fn list_access_value(
    list: &ListView<'_>,
    keys: &datafusion::arrow::array::ArrayRef,
    row: usize,
    null_value: &ScalarValue,
) -> datafusion::error::Result<ScalarValue> {
    if list.is_null(row) {
        return Ok(null_value.clone());
    }
    let key = ScalarValue::try_from_array(keys, row)?;
    let Some(idx) = scalar_list_index(&key)? else {
        return Ok(null_value.clone());
    };
    let elems = list.value(row);
    let len = i64::try_from(elems.len()).map_err(|_| {
        datafusion::error::DataFusionError::Execution(
            "dynamic list access length exceeds i64 range".into(),
        )
    })?;
    let pos = if idx < 0 { len + idx } else { idx };
    if pos < 0 || pos >= len {
        return Ok(null_value.clone());
    }
    let pos = usize::try_from(pos).map_err(|_| {
        datafusion::error::DataFusionError::Execution(
            "dynamic list access index exceeds usize range".into(),
        )
    })?;
    ScalarValue::try_from_array(&elems, pos)
}

fn scalar_list_index(s: &ScalarValue) -> datafusion::error::Result<Option<i64>> {
    if s.is_null() {
        return Ok(None);
    }
    macro_rules! signed_index {
        ($value:expr) => {
            $value.map(i64::from)
        };
    }
    let idx = match s {
        ScalarValue::Int8(v) => signed_index!(*v),
        ScalarValue::Int16(v) => signed_index!(*v),
        ScalarValue::Int32(v) => signed_index!(*v),
        ScalarValue::Int64(v) => *v,
        ScalarValue::UInt8(v) => v.map(i64::from),
        ScalarValue::UInt16(v) => v.map(i64::from),
        ScalarValue::UInt32(v) => v.map(i64::from),
        ScalarValue::UInt64(v) => v.map(i64::try_from).transpose().map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "dynamic list access index exceeds i64 range".into(),
            )
        })?,
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "dynamic list access index must be an integer, got {other:?}"
            )));
        }
    };
    Ok(idx)
}

fn het_value_access_return_type(dt: &DataType) -> datafusion::error::Result<DataType> {
    use datafusion::error::DataFusionError;

    let DataType::Struct(fields) = dt else {
        unreachable!("caller checked het struct type")
    };
    let Some(map_field) = fields
        .iter()
        .find(|f| f.name() == graphforge_value::heterogeneous::MAP)
    else {
        return Err(DataFusionError::Plan(
            "dynamic value access requires a tagged map element".into(),
        ));
    };
    let DataType::List(entry_field) = map_field.data_type() else {
        return Err(DataFusionError::Plan(
            "tagged map field must be a list".into(),
        ));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err(DataFusionError::Plan(
            "tagged map entries must be structs".into(),
        ));
    };
    entry_fields
        .iter()
        .find(|f| f.name() == graphforge_value::heterogeneous::MAP_VALUE)
        .map(|f| f.data_type().clone())
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must carry __het_mval".into()))
}

fn het_map_access_value(
    map: &datafusion::arrow::array::StructArray,
    keys: &datafusion::arrow::array::ArrayRef,
    row: usize,
    null_value: &ScalarValue,
) -> datafusion::error::Result<ScalarValue> {
    use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray};
    use datafusion::error::DataFusionError;

    if map.is_null(row) {
        return Ok(null_value.clone());
    }
    let key = ScalarValue::try_from_array(keys, row)?;
    let Some(key) = scalar_access_key(&key)? else {
        return Ok(null_value.clone());
    };
    if !matches!(
        graphforge_value::heterogeneous::decode_row(map, row)
            .map_err(|error| DataFusionError::External(Box::new(error)))?,
        graphforge_value::heterogeneous::Decoded::Map(_)
    ) {
        return Err(DataFusionError::Execution(
            "invalid argument type: dynamic value access requires a map".into(),
        ));
    }
    let entries = map
        .column_by_name(graphforge_value::heterogeneous::MAP)
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| DataFusionError::Plan("tagged map value is missing __het_map".into()))?;
    if entries.is_null(row) {
        return Ok(null_value.clone());
    }
    let entry_values = entries.value(row);
    let entry_struct = entry_values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must be structs".into()))?;
    let map_keys = entry_struct
        .column_by_name(graphforge_value::heterogeneous::MAP_KEY)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must carry __het_mkey".into()))?;
    let map_values = entry_struct
        .column_by_name(graphforge_value::heterogeneous::MAP_VALUE)
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must carry __het_mval".into()))?;
    for idx in 0..entry_struct.len() {
        if !map_keys.is_null(idx) && map_keys.value(idx) == key {
            return ScalarValue::try_from_array(map_values, idx);
        }
    }
    Ok(null_value.clone())
}

fn common_struct_field_type(
    fields: &datafusion::arrow::datatypes::Fields,
) -> datafusion::error::Result<DataType> {
    let mut dtype: Option<DataType> = None;
    for field in fields {
        let field_type = field.data_type();
        if matches!(field_type, DataType::Null) {
            continue;
        }
        match &dtype {
            None => dtype = Some(field_type.clone()),
            Some(prev) if prev == field_type => {}
            Some(prev) => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "dynamic value access over mixed field types is not supported: {prev:?} and {field_type:?}"
                )));
            }
        }
    }
    Ok(dtype.unwrap_or(DataType::Null))
}

fn scalar_access_key(s: &ScalarValue) -> datafusion::error::Result<Option<String>> {
    if s.is_null() {
        return Ok(None);
    }
    match s {
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            Ok(v.clone())
        }
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "dynamic map/property access key must be a string, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{invoke_test_udf, invoke_test_udf_with_return_type};
    use super::super::*;
    use super::*;

    #[test]
    fn public_map_and_value_access_error_null_and_success_matrix() {
        use datafusion::arrow::array::{Array, ListArray};

        let null_keys = invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Null]).unwrap();
        let null_keys = null_keys
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("List");
        assert!(null_keys.is_null(0));

        let keys_error =
            invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Int64(Some(1))]).unwrap_err();
        assert_eq!(
            keys_error.to_string(),
            "Execution error: keys() requires a map, node, relationship, or null, got Int64"
        );

        let map = const_map_scalar(&[
            ("answer".into(), ScalarValue::Int64(Some(42))),
            ("empty".into(), ScalarValue::Null),
        ])
        .expect("map scalar");
        let keys = invoke_test_udf(&CypherMapKeys::new(), vec![map.clone()]).unwrap();
        let keys = keys.as_any().downcast_ref::<ListArray>().expect("List");
        assert_eq!(keys.value(0).len(), 2);

        let answer = invoke_test_udf(
            &CypherStaticValueAccess::new("answer".into()),
            vec![map.clone()],
        )
        .unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&answer, 0).unwrap(),
            ScalarValue::Int64(Some(42))
        );
        let missing =
            invoke_test_udf(&CypherStaticValueAccess::new("missing".into()), vec![map]).unwrap();
        assert!(ScalarValue::try_from_array(&missing, 0).unwrap().is_null());

        let static_error = invoke_test_udf(
            &CypherStaticValueAccess::new("answer".into()),
            vec![ScalarValue::Int64(Some(1))],
        )
        .unwrap_err();
        assert_eq!(
            static_error.to_string(),
            "Execution error: InvalidArgumentValue: property access requires a map or graph element"
        );

        let dynamic_error = invoke_test_udf(
            &CypherValueAccess::new(),
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("answer".into())),
            ],
        )
        .unwrap_err();
        assert_eq!(
            dynamic_error.to_string(),
            "Execution error: dynamic subscript requires a list or map/entity struct, got Int64"
        );
    }

    #[test]
    fn entity_properties_validation_errors_are_exact() {
        let arity_error = invoke_test_udf(&CypherEntityProperties::new(0), vec![]).unwrap_err();
        assert_eq!(
            arity_error.to_string(),
            "Error during planning: properties() entity map expects present plus key/value pairs"
        );
        let presence_error = invoke_test_udf(
            &CypherEntityProperties::new(3),
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("name".into())),
                ScalarValue::Utf8(Some("Ada".into())),
            ],
        )
        .unwrap_err();
        assert_eq!(
            presence_error.to_string(),
            "Execution error: properties() entity presence must be boolean, got Int64(1)"
        );
    }

    #[test]
    fn dynamic_access_helpers_cover_null_bounds_types_and_schema_errors() {
        use datafusion::arrow::datatypes::{Field, Fields};

        for (scalar, expected) in [
            (ScalarValue::Int8(Some(-1)), Some(-1)),
            (ScalarValue::Int16(Some(2)), Some(2)),
            (ScalarValue::Int32(Some(3)), Some(3)),
            (ScalarValue::Int64(Some(4)), Some(4)),
            (ScalarValue::UInt8(Some(5)), Some(5)),
            (ScalarValue::UInt16(Some(6)), Some(6)),
            (ScalarValue::UInt32(Some(7)), Some(7)),
            (ScalarValue::UInt64(Some(8)), Some(8)),
            (ScalarValue::Null, None),
        ] {
            assert_eq!(scalar_list_index(&scalar).unwrap(), expected);
        }
        assert!(scalar_list_index(&ScalarValue::UInt64(Some(u64::MAX))).is_err());
        assert!(scalar_list_index(&ScalarValue::Utf8(Some("one".into()))).is_err());
        assert_eq!(scalar_access_key(&ScalarValue::Null).unwrap(), None);
        assert_eq!(
            scalar_access_key(&ScalarValue::LargeUtf8(Some("key".into()))).unwrap(),
            Some("key".into())
        );
        assert!(scalar_access_key(&ScalarValue::Int64(Some(1))).is_err());

        let homogeneous = Fields::from(vec![
            Field::new("a", DataType::Null, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, false),
        ]);
        assert_eq!(
            common_struct_field_type(&homogeneous).unwrap(),
            DataType::Int64
        );
        let mixed = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        assert!(common_struct_field_type(&mixed).is_err());

        for dtype in [
            DataType::Struct(Fields::empty()),
            DataType::Struct(Fields::from(vec![Field::new(
                graphforge_value::heterogeneous::MAP,
                DataType::Utf8,
                true,
            )])),
            DataType::Struct(Fields::from(vec![Field::new(
                graphforge_value::heterogeneous::MAP,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )])),
        ] {
            assert!(het_value_access_return_type(&dtype).is_err());
        }
    }

    #[test]
    fn heterogeneous_map_access_returns_exact_values_and_rejects_non_maps() {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::scalar::ScalarValue as S;

        let map = const_map_scalar(&[
            ("answer".to_owned(), S::Int64(Some(42))),
            ("empty".to_owned(), S::Int64(None)),
        ])
        .expect("map scalar");
        let encoded = build_het_struct(&[map], 1).expect("tagged map");
        let return_type =
            het_value_access_return_type(encoded.data_type()).expect("map value type");
        let null_value = S::try_from(&return_type).expect("typed null");
        let keys = |key: Option<&str>| -> ArrayRef { Arc::new(StringArray::from(vec![key])) };

        let found = het_map_access_value(&encoded, &keys(Some("answer")), 0, &null_value)
            .expect("existing map key");
        assert_eq!(decode_het(&found), Some(S::Int64(Some(42))));

        let stored_null = het_map_access_value(&encoded, &keys(Some("empty")), 0, &null_value)
            .expect("stored null");
        assert_eq!(decode_het(&stored_null), Some(S::Null));
        assert_eq!(
            het_map_access_value(&encoded, &keys(Some("missing")), 0, &null_value)
                .expect("missing key"),
            null_value
        );
        assert_eq!(
            het_map_access_value(&encoded, &keys(None), 0, &null_value).expect("null key"),
            null_value
        );

        let non_map = build_het_struct(&[S::Int64(Some(7))], 0).expect("tagged integer");
        let error = het_map_access_value(&non_map, &keys(Some("answer")), 0, &null_value)
            .expect_err("a tagged integer is not dynamically property-readable");
        assert_eq!(
            error.to_string(),
            "Execution error: invalid argument type: dynamic value access requires a map"
        );
    }

    #[test]
    fn dynamic_struct_access_observes_missing_null_type_and_row_null_semantics() {
        use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
        use datafusion::arrow::buffer::NullBuffer;
        use datafusion::arrow::datatypes::{Field, Fields};
        use datafusion::config::ConfigOptions;

        let values: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("score", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![Some(9), None, Some(11)]))],
            Some(NullBuffer::from(vec![true, true, false])),
        ));
        let invoke = |keys: ArrayRef| -> datafusion::error::Result<ArrayRef> {
            let udf = CypherValueAccess::new();
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::clone(&values)),
                    ColumnarValue::Array(keys),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("value", values.data_type().clone(), true)),
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                ],
                number_rows: 3,
                return_field: Arc::new(Field::new("out", DataType::Int64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })?;
            match result {
                ColumnarValue::Array(array) => Ok(array),
                ColumnarValue::Scalar(value) => value.to_array_of_size(3),
            }
        };

        let result = invoke(Arc::new(StringArray::from(vec![
            Some("score"),
            Some("missing"),
            Some("score"),
        ])))
        .expect("dynamic struct access");
        let result = result.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(result.value(0), 9);
        assert!(result.is_null(1), "an absent property is null");
        assert!(result.is_null(2), "a null graph-element row is null");

        let bad_keys: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let error = invoke(bad_keys).expect_err("numeric property key");
        assert!(
            error
                .to_string()
                .contains("dynamic map/property access key must be a string"),
            "{error}"
        );
    }

    #[test]
    fn exact_zero_access_map_metadata_and_type_helpers() {
        use datafusion::arrow::datatypes::{Field, Fields};

        let map = const_map_scalar(&[
            ("k".into(), ScalarValue::Int64(Some(7))),
            ("other".into(), ScalarValue::Int64(None)),
        ])
        .unwrap();
        let accessed =
            invoke_test_udf(&CypherStaticValueAccess::new("k".into()), vec![map.clone()]).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&accessed, 0).unwrap(),
            ScalarValue::Int64(Some(7))
        );
        let missing = invoke_test_udf(
            &CypherStaticValueAccess::new("missing".into()),
            vec![map.clone()],
        )
        .unwrap();
        assert!(ScalarValue::try_from_array(&missing, 0).unwrap().is_null());

        let keys = invoke_test_udf(&CypherMapKeys::new(), vec![map]).unwrap();
        let ScalarValue::List(keys) = ScalarValue::try_from_array(&keys, 0).unwrap() else {
            panic!("keys must return a list")
        };
        assert_eq!(keys.value(0).len(), 2);

        let props = invoke_test_udf(
            &CypherEntityProperties::new(3),
            vec![
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Utf8(Some("k".into())),
                ScalarValue::Int64(Some(7)),
            ],
        )
        .unwrap();
        assert!(!props.is_null(0));
        let absent = invoke_test_udf(
            &CypherEntityProperties::new(3),
            vec![
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Utf8(Some("k".into())),
                ScalarValue::Int64(Some(7)),
            ],
        )
        .unwrap();
        assert!(absent.is_null(0));

        let null_access = invoke_test_udf(
            &CypherValueAccess::new(),
            vec![ScalarValue::Null, ScalarValue::Utf8(Some("k".into()))],
        )
        .unwrap();
        assert!(
            ScalarValue::try_from_array(&null_access, 0)
                .unwrap()
                .is_null()
        );

        for (value, expected) in [
            (ScalarValue::Int8(Some(-1)), Some(-1)),
            (ScalarValue::Int16(Some(-2)), Some(-2)),
            (ScalarValue::Int32(Some(-3)), Some(-3)),
            (ScalarValue::Int64(Some(-4)), Some(-4)),
            (ScalarValue::UInt8(Some(1)), Some(1)),
            (ScalarValue::UInt16(Some(2)), Some(2)),
            (ScalarValue::UInt32(Some(3)), Some(3)),
            (ScalarValue::UInt64(Some(4)), Some(4)),
            (ScalarValue::Int64(None), None),
        ] {
            assert_eq!(scalar_list_index(&value).unwrap(), expected);
        }
        assert!(scalar_list_index(&ScalarValue::UInt64(Some(u64::MAX))).is_err());
        assert!(scalar_list_index(&ScalarValue::Utf8(Some("0".into()))).is_err());

        let nested_a = DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("items", DataType::new_list(DataType::Utf8, true), true),
        ]));
        let nested_b = DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("items", DataType::new_list(DataType::Utf8, true), false),
        ]));
        assert!(graph_value_types_compatible(&nested_a, &nested_b));
        assert!(!graph_value_types_compatible(&nested_a, &DataType::Int64));
        assert!(!graph_value_types_compatible(
            &nested_a,
            &DataType::Struct(Fields::from(vec![Field::new(
                "other",
                DataType::Int64,
                true
            )]))
        ));
        let unified = unify_graph_value_nullability(&nested_a, &nested_b).unwrap();
        let DataType::Struct(fields) = &unified else {
            panic!("struct");
        };
        assert!(fields[0].is_nullable(), "value nullability is widened");
        assert!(fields[1].is_nullable(), "items nullability is widened");
        let non_null_uuid = DataType::Struct(Fields::from(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let nullable_uuid = DataType::Struct(Fields::from(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            true,
        )]));
        let list_target = unify_graph_value_nullability(
            &DataType::new_list(non_null_uuid.clone(), true),
            &DataType::new_list(nullable_uuid.clone(), true),
        )
        .unwrap();
        let DataType::List(item) = &list_target else {
            panic!("list");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("struct element");
        };
        assert!(
            fields[0].is_nullable(),
            "DF54 path-list concat must widen node_uuid nullability"
        );
        let large = unify_graph_value_nullability(
            &DataType::new_large_list(non_null_uuid.clone(), true),
            &DataType::new_large_list(nullable_uuid.clone(), true),
        )
        .unwrap();
        assert!(matches!(large, DataType::LargeList(_)));
        let fixed = unify_graph_value_nullability(
            &DataType::new_fixed_size_list(non_null_uuid, 2, true),
            &DataType::new_fixed_size_list(nullable_uuid, 2, true),
        )
        .unwrap();
        assert!(matches!(fixed, DataType::FixedSizeList(_, 2)));
        assert!(
            unify_graph_value_nullability(
                &DataType::new_fixed_size_list(
                    DataType::Struct(Fields::from(vec![Field::new(
                        "node_uuid",
                        DataType::FixedSizeBinary(16),
                        false,
                    )])),
                    2,
                    true,
                ),
                &DataType::new_fixed_size_list(
                    DataType::Struct(Fields::from(vec![Field::new(
                        "node_uuid",
                        DataType::FixedSizeBinary(16),
                        true,
                    )])),
                    3,
                    true,
                ),
            )
            .is_none(),
            "FixedSizeList widths must match"
        );

        for (name, value) in [
            ("date", "2020-01-02"),
            ("localtime", "12:34:56"),
            ("time", "12:34:56Z"),
            ("localdatetime", "2020-01-02T12:34:56"),
            ("datetime", "2020-01-02T12:34:56Z"),
            ("duration", "P1D"),
        ] {
            assert!(render_temporal(name, value).is_some());
        }
        assert_eq!(render_temporal("unknown", "P1D"), None);
    }

    #[test]
    fn exact_zero_dynamic_list_access_supports_negative_null_and_missing_indexes() {
        let values = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Utf8(Some("first".into())),
                ScalarValue::Utf8(Some("second".into())),
            ],
            &DataType::Utf8,
            true,
        ));
        for (index, expected) in [
            (ScalarValue::Int64(Some(0)), Some("first")),
            (ScalarValue::Int64(Some(-1)), Some("second")),
            (ScalarValue::Int64(Some(9)), None),
            (ScalarValue::Int64(Some(-9)), None),
            (ScalarValue::Int64(None), None),
        ] {
            let output =
                invoke_test_udf(&CypherValueAccess::new(), vec![values.clone(), index]).unwrap();
            assert_eq!(
                ScalarValue::try_from_array(&output, 0).unwrap(),
                ScalarValue::Utf8(expected.map(str::to_owned))
            );
        }
        assert!(
            invoke_test_udf(
                &CypherValueAccess::new(),
                vec![values, ScalarValue::Utf8(Some("not-an-index".into())),],
            )
            .unwrap_err()
            .to_string()
            .contains("index must be an integer")
        );
    }

    #[test]
    fn exact_zero_map_and_subscript_error_guards_are_precise() {
        let null_keys = invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Null]).unwrap();
        assert!(null_keys.is_null(0));
        assert!(
            invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Int64(Some(1))])
                .unwrap_err()
                .to_string()
                .contains("keys() requires a map")
        );
        assert!(
            invoke_test_udf(
                &CypherStaticValueAccess::new("key".into()),
                vec![ScalarValue::Int64(Some(1))],
            )
            .unwrap_err()
            .to_string()
            .contains("property access requires a map")
        );
        assert!(
            invoke_test_udf(
                &CypherValueAccess::new(),
                vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(0)),],
            )
            .unwrap_err()
            .to_string()
            .contains("requires a list or map")
        );

        let map = const_map_scalar(&[("key".into(), ScalarValue::Int64(Some(7)))]).unwrap();
        let mismatch = invoke_test_udf_with_return_type(
            &CypherStaticValueAccess::new("key".into()),
            vec![map],
            DataType::Utf8,
        )
        .unwrap_err();
        assert!(mismatch.to_string().contains("incompatible runtime type"));

        assert_eq!(value_access_return_type(None).unwrap(), DataType::Null);
        assert_eq!(
            value_access_return_type(Some(&DataType::Null)).unwrap(),
            DataType::Null
        );
        assert_eq!(
            value_access_return_type(Some(&DataType::new_list(DataType::Int64, true))).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            static_value_access_return_type(None, "key").unwrap(),
            DataType::Null
        );
    }
}
