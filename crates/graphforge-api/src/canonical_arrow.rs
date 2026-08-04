//! Canonical logical Arrow table bytes for durable result fingerprints.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalError, CanonicalWriter, fingerprint,
};

/// Canonicalize logical rows and compute the `graphforge/arrow-result` digest.
pub(crate) fn result_fingerprint(batches: &[RecordBatch]) -> Result<[u8; 32], CanonicalArrowError> {
    Ok(fingerprint(
        CanonicalDomain::ArrowResult,
        CANONICAL_CONTRACT_VERSION,
        &canonical_table_bytes(batches)?,
    )?)
}

fn canonical_table_bytes(batches: &[RecordBatch]) -> Result<Vec<u8>, CanonicalArrowError> {
    let schema = batches
        .first()
        .map_or_else(|| std::sync::Arc::new(Schema::empty()), RecordBatch::schema);
    let schema_bytes = canonical_schema_bytes(schema.as_ref())?;
    for batch in batches {
        if canonical_schema_bytes(batch.schema().as_ref())? != schema_bytes {
            return Err(CanonicalArrowError::Schema(
                "record batches have different logical schemas",
            ));
        }
    }
    let row_count = batches.iter().try_fold(0_u64, |total, batch| {
        total
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| CanonicalArrowError::Schema("Arrow row count exceeds UInt64"))?,
            )
            .ok_or(CanonicalArrowError::Schema(
                "Arrow row count exceeds UInt64",
            ))
    })?;
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFT1")?;
    writer
        .u64(u64::try_from(schema_bytes.len()).map_err(|_| {
            CanonicalArrowError::Schema("canonical schema length exceeds UInt64")
        })?)?;
    writer.raw(&schema_bytes)?;
    writer.u64(row_count)?;
    for batch in batches {
        let batch_schema = batch.schema();
        let columns = batch_schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| {
                let logical_type = dictionary_value_type(field.data_type());
                if logical_type == field.data_type() {
                    Ok((logical_type, Arc::clone(column)))
                } else {
                    arrow::compute::cast(column, logical_type)
                        .map(|decoded| (logical_type, decoded))
                        .map_err(CanonicalArrowError::from)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            for (field, (logical_type, column)) in batch_schema.fields().iter().zip(&columns) {
                encode_value(&mut writer, logical_type, column, row, field.is_nullable())?;
            }
        }
    }
    Ok(writer.finish())
}

fn dictionary_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value,
        _ => data_type,
    }
}

fn canonical_schema_bytes(schema: &Schema) -> Result<Vec<u8>, CanonicalArrowError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFS1")?;
    writer.u32(exact_u32(schema.fields().len(), "field count")?)?;
    for field in &schema.fields {
        encode_field(&mut writer, field)?;
    }
    encode_metadata(&mut writer, schema.metadata())?;
    Ok(writer.finish())
}

fn encode_field(writer: &mut CanonicalWriter, field: &Field) -> Result<(), CanonicalArrowError> {
    writer.text(field.name())?;
    writer.u8(u8::from(field.is_nullable()))?;
    encode_type(writer, field.data_type())?;
    encode_metadata(writer, field.metadata())?;
    Ok(())
}

fn encode_metadata(
    writer: &mut CanonicalWriter,
    metadata: &std::collections::HashMap<String, String>,
) -> Result<(), CanonicalArrowError> {
    let mut ordered = BTreeMap::new();
    for (key, value) in metadata {
        match metadata_class(key) {
            MetadataClass::Semantic => {
                ordered.insert(key, value);
            }
            MetadataClass::Volatile => {}
            MetadataClass::UnknownGraphForge => {
                return Err(CanonicalArrowError::UnknownMetadata(key.clone()));
            }
        }
    }
    writer.u32(exact_u32(ordered.len(), "metadata entry count")?)?;
    for (key, value) in ordered {
        writer.text(key)?;
        writer.text(value)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum MetadataClass {
    Semantic,
    Volatile,
    UnknownGraphForge,
}

fn metadata_class(key: &str) -> MetadataClass {
    if key.starts_with("graphforge.contract.")
        || matches!(
            key,
            "graphforge.algorithm"
                | "graphforge.algorithm_version"
                | "graphforge.algorithm_schema_version"
                | "graphforge.verb"
                | "graphforge.search_schema_version"
                | "graphforge.dimensions"
                | "graphforge.seed"
                | "graphforge.rng_version"
                | "graphforge.rng_derivation"
                | "graphforge.valid_time.policy"
                | "graphforge.base_snapshot_unchanged"
        )
    {
        MetadataClass::Semantic
    } else if !key.starts_with("graphforge.")
        || matches!(
            key,
            "graphforge.query_id" | "graphforge.ontology_mode" | "graphforge.ontology_version"
        )
    {
        MetadataClass::Volatile
    } else {
        MetadataClass::UnknownGraphForge
    }
}

fn encode_type(
    writer: &mut CanonicalWriter,
    data_type: &DataType,
) -> Result<(), CanonicalArrowError> {
    match data_type {
        DataType::Boolean => writer.u8(0x02)?,
        DataType::Int32 => writer.u8(0x12)?,
        DataType::Int64 => writer.u8(0x13)?,
        DataType::UInt32 => writer.u8(0x16)?,
        DataType::UInt64 => writer.u8(0x17)?,
        DataType::Float32 => writer.u8(0x21)?,
        DataType::Float64 => writer.u8(0x22)?,
        DataType::Utf8 | DataType::LargeUtf8 => writer.u8(0x30)?,
        DataType::Binary | DataType::LargeBinary => writer.u8(0x31)?,
        DataType::FixedSizeBinary(width) => {
            writer.u8(0x32)?;
            writer
                .u32(u32::try_from(*width).map_err(|_| {
                    CanonicalArrowError::Schema("fixed binary width is negative")
                })?)?;
        }
        DataType::Timestamp(_, timezone) => {
            writer.u8(0x52)?;
            validate_timestamp_timezone(timezone.as_deref())?;
            writer.u8(u8::from(timezone.is_some()))?;
        }
        DataType::List(field) | DataType::LargeList(field) => {
            writer.u8(0x60)?;
            encode_field(writer, field)?;
        }
        DataType::FixedSizeList(field, length) => {
            writer.u8(0x61)?;
            writer.u32(
                u32::try_from(*length)
                    .map_err(|_| CanonicalArrowError::Schema("fixed list length is negative"))?,
            )?;
            encode_field(writer, field)?;
        }
        DataType::Struct(fields) => {
            writer.u8(0x62)?;
            writer.u32(exact_u32(fields.len(), "struct child count")?)?;
            for field in fields {
                encode_field(writer, field)?;
            }
        }
        DataType::Dictionary(_, value) => encode_type(writer, value)?,
        _ => return Err(CanonicalArrowError::Unsupported(data_type.clone())),
    }
    Ok(())
}

fn encode_value(
    writer: &mut CanonicalWriter,
    data_type: &DataType,
    array: &ArrayRef,
    row: usize,
    nullable: bool,
) -> Result<(), CanonicalArrowError> {
    if array.is_null(row) {
        if !nullable {
            return Err(CanonicalArrowError::Schema(
                "non-nullable Arrow field contains null",
            ));
        }
        writer.u8(0)?;
        return Ok(());
    }
    writer.u8(1)?;
    encode_present_value(writer, data_type, array, row)
}

#[allow(clippy::too_many_lines)]
fn encode_present_value(
    writer: &mut CanonicalWriter,
    data_type: &DataType,
    array: &ArrayRef,
    row: usize,
) -> Result<(), CanonicalArrowError> {
    match data_type {
        DataType::Boolean => writer.u8(u8::from(downcast::<BooleanArray>(array)?.value(row)))?,
        DataType::Int32 => writer.raw(&downcast::<Int32Array>(array)?.value(row).to_be_bytes())?,
        DataType::Int64 => writer.i64(downcast::<Int64Array>(array)?.value(row))?,
        DataType::UInt32 => writer.u32(downcast::<UInt32Array>(array)?.value(row))?,
        DataType::UInt64 => writer.u64(downcast::<UInt64Array>(array)?.value(row))?,
        DataType::Float32 => {
            let value = downcast::<Float32Array>(array)?.value(row);
            writer.u32(normalize_f32(value))?;
        }
        DataType::Float64 => {
            let value = downcast::<Float64Array>(array)?.value(row);
            writer.u64(normalize_f64(value))?;
        }
        DataType::Utf8 => writer.text(downcast::<StringArray>(array)?.value(row))?,
        DataType::LargeUtf8 => writer.text(downcast::<LargeStringArray>(array)?.value(row))?,
        DataType::Binary => writer.binary(downcast::<BinaryArray>(array)?.value(row))?,
        DataType::LargeBinary => writer.binary(downcast::<LargeBinaryArray>(array)?.value(row))?,
        DataType::FixedSizeBinary(_) => {
            writer.raw(downcast::<FixedSizeBinaryArray>(array)?.value(row))?;
        }
        DataType::Timestamp(unit, timezone) => {
            validate_timestamp_timezone(timezone.as_deref())?;
            let value = timestamp_value(array, *unit, row)?;
            writer.i64(to_microseconds(value, *unit)?)?;
        }
        DataType::List(field) => {
            let values = downcast::<ListArray>(array)?.value(row);
            encode_list(writer, field, &values)?;
        }
        DataType::LargeList(field) => {
            let values = downcast::<LargeListArray>(array)?.value(row);
            encode_list(writer, field, &values)?;
        }
        DataType::FixedSizeList(field, _) => {
            let values = downcast::<FixedSizeListArray>(array)?.value(row);
            for index in 0..values.len() {
                encode_value(
                    writer,
                    field.data_type(),
                    &values,
                    index,
                    field.is_nullable(),
                )?;
            }
        }
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array)?;
            for (field, child) in fields.iter().zip(values.columns()) {
                encode_value(writer, field.data_type(), child, row, field.is_nullable())?;
            }
        }
        _ => return Err(CanonicalArrowError::Unsupported(data_type.clone())),
    }
    Ok(())
}

fn encode_list(
    writer: &mut CanonicalWriter,
    field: &Field,
    values: &ArrayRef,
) -> Result<(), CanonicalArrowError> {
    writer.u64(
        u64::try_from(values.len())
            .map_err(|_| CanonicalArrowError::Schema("list length exceeds UInt64"))?,
    )?;
    for index in 0..values.len() {
        encode_value(
            writer,
            field.data_type(),
            values,
            index,
            field.is_nullable(),
        )?;
    }
    Ok(())
}

fn downcast<T: 'static>(array: &ArrayRef) -> Result<&T, CanonicalArrowError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(CanonicalArrowError::Schema("Arrow array/type mismatch"))
}

fn timestamp_value(
    array: &ArrayRef,
    unit: TimeUnit,
    row: usize,
) -> Result<i64, CanonicalArrowError> {
    macro_rules! value {
        ($ty:ty) => {
            downcast::<$ty>(array)?.value(row)
        };
    }
    Ok(match unit {
        TimeUnit::Second => value!(arrow::array::TimestampSecondArray),
        TimeUnit::Millisecond => value!(arrow::array::TimestampMillisecondArray),
        TimeUnit::Microsecond => value!(arrow::array::TimestampMicrosecondArray),
        TimeUnit::Nanosecond => value!(arrow::array::TimestampNanosecondArray),
    })
}

fn to_microseconds(value: i64, unit: TimeUnit) -> Result<i64, CanonicalArrowError> {
    match unit {
        TimeUnit::Second => value
            .checked_mul(1_000_000)
            .ok_or(CanonicalArrowError::Temporal),
        TimeUnit::Millisecond => value
            .checked_mul(1_000)
            .ok_or(CanonicalArrowError::Temporal),
        TimeUnit::Microsecond => Ok(value),
        TimeUnit::Nanosecond if value % 1_000 == 0 => Ok(value / 1_000),
        TimeUnit::Nanosecond => Err(CanonicalArrowError::Temporal),
    }
}

fn validate_timestamp_timezone(timezone: Option<&str>) -> Result<(), CanonicalArrowError> {
    if timezone.is_none_or(|value| matches!(value, "UTC" | "Etc/UTC" | "Z" | "+00:00")) {
        Ok(())
    } else {
        Err(CanonicalArrowError::Temporal)
    }
}

fn normalize_f32(value: f32) -> u32 {
    if value.is_nan() {
        0x7fc0_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn normalize_f64(value: f64) -> u64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn exact_u32(value: usize, item: &'static str) -> Result<u32, CanonicalArrowError> {
    u32::try_from(value).map_err(|_| CanonicalArrowError::Schema(item))
}

/// Structured canonical Arrow failure.
#[derive(thiserror::Error, Debug)]
pub(crate) enum CanonicalArrowError {
    /// Shared bounded canonical byte failure.
    #[error(transparent)]
    Canonical(#[from] CanonicalError),
    /// Arrow physical access or dictionary decoding failed.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// Logical schema or array content is inconsistent.
    #[error("invalid canonical Arrow table: {0}")]
    Schema(&'static str),
    /// The v1 type registry does not include this logical type.
    #[error("unsupported canonical Arrow type: {0}")]
    Unsupported(DataType),
    /// A GraphForge metadata key has no semantic/volatile classification.
    #[error("unclassified GraphForge Arrow metadata key: {0}")]
    UnknownMetadata(String),
    /// Temporal conversion was not exact or used a non-UTC zone.
    #[error("Arrow temporal value cannot be represented exactly in canonical microseconds")]
    Temporal,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::{Field, Int8Type, Schema};

    use super::*;

    #[test]
    fn result_fingerprint_ignores_batches_and_dictionary_layout() {
        let plain_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let plain = RecordBatch::try_new(
            Arc::clone(&plain_schema),
            vec![Arc::new(StringArray::from(vec!["a", "b", "a"]))],
        )
        .unwrap();
        let dictionary: DictionaryArray<Int8Type> = DictionaryArray::try_new(
            Int8Array::from(vec![1, 0, 1]),
            Arc::new(StringArray::from(vec!["b", "a"])),
        )
        .unwrap();
        let dictionary_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                dictionary.data_type().clone(),
                false,
            )])),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        assert_eq!(
            result_fingerprint(&[plain.clone()]).unwrap(),
            result_fingerprint(&[plain.slice(0, 1), plain.slice(1, 2)]).unwrap()
        );
        assert_eq!(
            result_fingerprint(&[plain]).unwrap(),
            result_fingerprint(&[dictionary_batch]).unwrap()
        );
    }

    #[test]
    fn result_fingerprint_filters_metadata_by_the_closed_registry() {
        let batch = |metadata: std::collections::HashMap<String, String>| {
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    vec![Field::new("value", DataType::Utf8, false)],
                    metadata,
                )),
                vec![Arc::new(StringArray::from(vec!["a"]))],
            )
            .unwrap()
        };
        let base = batch(std::collections::HashMap::from([(
            "graphforge.algorithm".to_owned(),
            "pagerank".to_owned(),
        )]));
        let volatile = batch(std::collections::HashMap::from([
            ("graphforge.algorithm".to_owned(), "pagerank".to_owned()),
            ("graphforge.query_id".to_owned(), "different".to_owned()),
            ("producer-build".to_owned(), "ignored".to_owned()),
        ]));
        let semantic_change = batch(std::collections::HashMap::from([(
            "graphforge.algorithm".to_owned(),
            "hits".to_owned(),
        )]));
        assert_eq!(
            result_fingerprint(&[base.clone()]).unwrap(),
            result_fingerprint(&[volatile]).unwrap()
        );
        assert_ne!(
            result_fingerprint(&[base]).unwrap(),
            result_fingerprint(&[semantic_change]).unwrap()
        );
        let unknown = batch(std::collections::HashMap::from([(
            "graphforge.extra".to_owned(),
            "unclassified".to_owned(),
        )]));
        assert!(matches!(
            result_fingerprint(&[unknown]),
            Err(CanonicalArrowError::UnknownMetadata(key)) if key == "graphforge.extra"
        ));
    }

    #[test]
    fn canonical_scalar_temporal_and_schema_boundaries_are_exact() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![-32])),
            Arc::new(Int64Array::from(vec![-64])),
            Arc::new(UInt32Array::from(vec![32])),
            Arc::new(UInt64Array::from(vec![64])),
            Arc::new(Float32Array::from(vec![1.5])),
            Arc::new(Float64Array::from(vec![2.5])),
            Arc::new(StringArray::from(vec!["utf8"])),
            Arc::new(LargeStringArray::from(vec!["large"])),
            Arc::new(BinaryArray::from(vec![b"binary".as_slice()])),
            Arc::new(LargeBinaryArray::from(vec![b"large-binary".as_slice()])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([b"0123456789abcdef".as_slice()].into_iter())
                    .unwrap(),
            ),
            Arc::new(arrow::array::TimestampSecondArray::from(vec![2])),
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![2_000])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                2_000_000,
            ])),
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                2_000_000_000,
            ])),
        ];
        for array in arrays {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    array.data_type().clone(),
                    false,
                )])),
                vec![array],
            )
            .unwrap();
            assert_ne!(result_fingerprint(&[batch]).unwrap(), [0; 32]);
        }

        for (left, right) in [(0.0, -0.0), (f64::NAN, -f64::NAN)] {
            let batch = |value| {
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Float64,
                        false,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![value]))],
                )
                .unwrap()
            };
            assert_eq!(
                result_fingerprint(&[batch(left)]).unwrap(),
                result_fingerprint(&[batch(right)]).unwrap()
            );
        }
        assert_eq!(to_microseconds(2, TimeUnit::Second).unwrap(), 2_000_000);
        assert_eq!(to_microseconds(2, TimeUnit::Millisecond).unwrap(), 2_000);
        assert_eq!(to_microseconds(2, TimeUnit::Microsecond).unwrap(), 2);
        assert_eq!(to_microseconds(2_000, TimeUnit::Nanosecond).unwrap(), 2);
        assert!(matches!(
            to_microseconds(1, TimeUnit::Nanosecond),
            Err(CanonicalArrowError::Temporal)
        ));
        assert!(matches!(
            to_microseconds(i64::MAX, TimeUnit::Second),
            Err(CanonicalArrowError::Temporal)
        ));
        assert!(validate_timestamp_timezone(Some("UTC")).is_ok());
        assert!(validate_timestamp_timezone(Some("America/Denver")).is_err());

        let utf8 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let integers = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(matches!(
            result_fingerprint(&[utf8, integers]),
            Err(CanonicalArrowError::Schema(
                "record batches have different logical schemas"
            ))
        ));
        let mut writer = CanonicalWriter::new();
        assert!(matches!(
            encode_type(&mut writer, &DataType::Date32),
            Err(CanonicalArrowError::Unsupported(DataType::Date32))
        ));
    }

    #[test]
    fn canonical_schema_rejects_invalid_nested_widths_and_preserves_semantic_metadata() {
        let child = Arc::new(Field::new("item", DataType::UInt64, true));
        let struct_fields =
            arrow::datatypes::Fields::from(vec![Field::new("child", DataType::Utf8, false)]);
        for data_type in [
            DataType::List(Arc::clone(&child)),
            DataType::LargeList(Arc::clone(&child)),
            DataType::FixedSizeList(Arc::clone(&child), 2),
            DataType::Struct(struct_fields),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        ] {
            let mut writer = CanonicalWriter::new();
            encode_type(&mut writer, &data_type).unwrap();
            assert!(!writer.finish().is_empty());
        }

        for data_type in [
            DataType::FixedSizeBinary(-1),
            DataType::FixedSizeList(Arc::clone(&child), -1),
        ] {
            let mut writer = CanonicalWriter::new();
            assert!(matches!(
                encode_type(&mut writer, &data_type),
                Err(CanonicalArrowError::Schema(_))
            ));
        }

        let metadata = std::collections::HashMap::from([
            ("graphforge.contract.result".to_owned(), "v1".to_owned()),
            ("graphforge.seed".to_owned(), "7".to_owned()),
            ("graphforge.ontology_mode".to_owned(), "strict".to_owned()),
        ]);
        let with_metadata =
            Schema::new_with_metadata(vec![Field::new("value", DataType::Utf8, false)], metadata);
        assert!(!canonical_schema_bytes(&with_metadata).unwrap().is_empty());
    }

    #[test]
    fn canonical_nullability_physical_mismatch_and_float_normalization_are_explicit() {
        let nulls: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let mut writer = CanonicalWriter::new();
        assert!(matches!(
            encode_value(&mut writer, &DataType::Utf8, &nulls, 0, false),
            Err(CanonicalArrowError::Schema(
                "non-nullable Arrow field contains null"
            ))
        ));
        let mut writer = CanonicalWriter::new();
        encode_value(&mut writer, &DataType::Utf8, &nulls, 0, true).unwrap();
        assert!(!writer.finish().is_empty());

        let integers: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut writer = CanonicalWriter::new();
        assert!(matches!(
            encode_present_value(&mut writer, &DataType::Utf8, &integers, 0),
            Err(CanonicalArrowError::Schema("Arrow array/type mismatch"))
        ));

        assert_eq!(normalize_f32(0.0), normalize_f32(-0.0));
        assert_eq!(normalize_f32(f32::NAN), normalize_f32(-f32::NAN));
        assert_ne!(normalize_f32(1.5), 0);
        assert_ne!(normalize_f64(1.5), 0);
        for timezone in [
            None,
            Some("UTC"),
            Some("Etc/UTC"),
            Some("Z"),
            Some("+00:00"),
        ] {
            assert!(validate_timestamp_timezone(timezone).is_ok());
        }
    }
}
