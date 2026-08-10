//! Bounded columnar Arrow sinks for analyst-verb outputs (#341).
//!
//! Handlers append canonical field values directly into typed builders. Finished
//! batches never retain a second complete `Vec<Vec<AlgorithmValue>>` copy.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    Float32Array, Float32Builder, Float64Array, Float64Builder, Int64Array, Int64Builder,
    ListArray, ListBuilder, StringArray, StringBuilder, UInt64Array, UInt64Builder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, AlgorithmFieldType, AlgorithmResultSchema};

use crate::algorithm_dispatch::{
    AlgorithmControl, AlgorithmError, AlgorithmOutput, AlgorithmValue,
};

const SCHEMA_VERSION: &str = "1";

/// Typed, bounded Arrow builder set for one algorithm result schema.
pub(crate) struct AlgorithmArrowSink {
    #[allow(dead_code, reason = "retained for diagnostics and future observability")]
    algorithm: Algorithm,
    schema: AlgorithmResultSchema,
    arrow_schema: Arc<Schema>,
    batch_size: usize,
    output_limit: u64,
    builders: Vec<ColumnBuilder>,
    current_rows: usize,
    total_rows: usize,
    finished: Vec<RecordBatch>,
    /// Peak logical rows retained in the active builder window (≤ batch_size).
    peak_builder_rows: usize,
}

enum ColumnBuilder {
    Uuid(FixedSizeBinaryBuilder),
    UuidList(ListBuilder<FixedSizeBinaryBuilder>),
    Float32List(ListBuilder<Float32Builder>),
    Utf8(StringBuilder),
    Boolean(BooleanBuilder),
    UInt64(UInt64Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
}

impl AlgorithmArrowSink {
    /// Start a sink for `algorithm` using the invocation's batch and row limits.
    pub(crate) fn new(
        algorithm: Algorithm,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        Self::with_limits(
            algorithm,
            control.batch_size(),
            control.configured_limits().output_rows,
        )
    }

    /// Start a sink with explicit batch size and output-row limit (tests / shaping).
    pub(crate) fn with_limits(
        algorithm: Algorithm,
        batch_size: usize,
        output_limit: u64,
    ) -> Result<Self, AlgorithmError> {
        let schema = algorithm.result_schema();
        let mut fields = Vec::with_capacity(schema.fields.len());
        let mut builders = Vec::with_capacity(schema.fields.len());
        for logical in schema.fields {
            if matches!(logical.name, "node_id" | "edge_id" | "src_id" | "dst_id") {
                return Err(shape_error(
                    "public algorithm schema contains a surrogate field",
                ));
            }
            fields.push(Field::new(
                logical.name,
                arrow_type(logical.data_type),
                logical.nullable,
            ));
            builders.push(ColumnBuilder::new(logical.data_type)?);
        }
        let metadata = HashMap::from([
            (
                "graphforge.algorithm".to_owned(),
                algorithm.as_str().to_owned(),
            ),
            (
                "graphforge.verb".to_owned(),
                algorithm.verb().as_str().to_owned(),
            ),
            (
                "graphforge.algorithm_schema_version".to_owned(),
                SCHEMA_VERSION.to_owned(),
            ),
        ]);
        Ok(Self {
            algorithm,
            schema,
            arrow_schema: Arc::new(Schema::new_with_metadata(fields, metadata)),
            batch_size: batch_size.max(1),
            output_limit,
            builders,
            current_rows: 0,
            total_rows: 0,
            finished: Vec::new(),
            peak_builder_rows: 0,
        })
    }

    pub(crate) fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    pub(crate) fn schema(&self) -> &AlgorithmResultSchema {
        &self.schema
    }

    #[cfg(test)]
    pub(crate) fn peak_builder_rows(&self) -> usize {
        self.peak_builder_rows
    }

    #[cfg(test)]
    pub(crate) fn internal_batch_count(&self) -> usize {
        self.finished.len() + usize::from(self.current_rows > 0)
    }

    /// Append one canonical logical row into the active builders.
    pub(crate) fn append_row(&mut self, row: &[AlgorithmValue]) -> Result<(), AlgorithmError> {
        if row.len() != self.schema.fields.len() {
            return Err(shape_error(format!(
                "row has {} values but schema requires {}",
                row.len(),
                self.schema.fields.len()
            )));
        }
        let next_total = self
            .total_rows
            .checked_add(1)
            .ok_or_else(|| shape_error("output row count exceeds usize"))?;
        let observed = u64::try_from(next_total).unwrap_or(u64::MAX);
        if observed > self.output_limit {
            return Err(AlgorithmError::OutputLimit {
                observed,
                limit: self.output_limit,
            });
        }
        if self.current_rows >= self.batch_size {
            self.flush_batch()?;
        }
        for (index, (field, value)) in self.schema.fields.iter().zip(row.iter()).enumerate() {
            self.builders[index].append(field.name, field.nullable, value)?;
        }
        self.current_rows += 1;
        self.total_rows = next_total;
        self.peak_builder_rows = self.peak_builder_rows.max(self.current_rows);
        Ok(())
    }

    /// Finish remaining builders and coalesce to the public single-batch contract.
    pub(crate) fn finish(mut self) -> Result<AlgorithmOutput, AlgorithmError> {
        if self.current_rows > 0 || self.finished.is_empty() {
            self.flush_batch()?;
        }
        let internal_batch_count = self.finished.len().max(1);
        let peak_builder_rows = self.peak_builder_rows;
        let batch = coalesce_batches(&self.arrow_schema, &self.finished)?;
        Ok(AlgorithmOutput {
            schema: self.schema,
            batch,
            internal_batch_count,
            peak_builder_rows,
        })
    }
}

impl ColumnBuilder {
    fn new(data_type: AlgorithmFieldType) -> Result<Self, AlgorithmError> {
        Ok(match data_type {
            AlgorithmFieldType::Uuid => Self::Uuid(FixedSizeBinaryBuilder::new(16)),
            AlgorithmFieldType::UuidList => {
                Self::UuidList(
                    ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(
                        Field::new("item", DataType::FixedSizeBinary(16), false),
                    )),
                )
            }
            AlgorithmFieldType::Float32List => Self::Float32List(
                ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
                    "item",
                    DataType::Float32,
                    false,
                ))),
            ),
            AlgorithmFieldType::Utf8 => Self::Utf8(StringBuilder::new()),
            AlgorithmFieldType::Boolean => Self::Boolean(BooleanBuilder::new()),
            AlgorithmFieldType::UInt64 => Self::UInt64(UInt64Builder::new()),
            AlgorithmFieldType::Int64 => Self::Int64(Int64Builder::new()),
            AlgorithmFieldType::Float64 => Self::Float64(Float64Builder::new()),
        })
    }

    fn append(
        &mut self,
        name: &str,
        nullable: bool,
        value: &AlgorithmValue,
    ) -> Result<(), AlgorithmError> {
        match (self, value) {
            (Self::Uuid(builder), AlgorithmValue::Uuid(value)) => builder
                .append_value(value)
                .map_err(|error| shape_error(error.to_string()))?,
            (Self::Uuid(builder), AlgorithmValue::Null) if nullable => builder.append_null(),
            (Self::UuidList(builder), AlgorithmValue::UuidList(values)) => {
                for value in values {
                    builder
                        .values()
                        .append_value(value)
                        .map_err(|error| shape_error(error.to_string()))?;
                }
                builder.append(true);
            }
            (Self::UuidList(builder), AlgorithmValue::Null) if nullable => builder.append(false),
            (Self::Float32List(builder), AlgorithmValue::Float32List(values))
                if values.iter().all(|value| value.is_finite()) =>
            {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            (Self::Float32List(_), AlgorithmValue::Float32List(_)) => {
                return Err(shape_error(format!(
                    "field {name:?} contains a non-finite Float32"
                )));
            }
            (Self::Float32List(builder), AlgorithmValue::Null) if nullable => builder.append(false),
            (Self::Utf8(builder), AlgorithmValue::Utf8(value)) => builder.append_value(value),
            (Self::Utf8(builder), AlgorithmValue::Null) if nullable => builder.append_null(),
            (Self::Boolean(builder), AlgorithmValue::Boolean(value)) => {
                builder.append_value(*value);
            }
            (Self::Boolean(builder), AlgorithmValue::Null) if nullable => builder.append_null(),
            (Self::UInt64(builder), AlgorithmValue::UInt64(value)) => builder.append_value(*value),
            (Self::UInt64(builder), AlgorithmValue::Null) if nullable => builder.append_null(),
            (Self::Int64(builder), AlgorithmValue::Int64(value)) => builder.append_value(*value),
            (Self::Int64(builder), AlgorithmValue::Null) if nullable => builder.append_null(),
            (Self::Float64(builder), AlgorithmValue::Float64(value)) if value.is_finite() => {
                builder.append_value(*value);
            }
            (Self::Float64(_), AlgorithmValue::Float64(_)) => {
                return Err(shape_error(format!(
                    "field {name:?} contains a non-finite Float64"
                )));
            }
            (Self::Float64(builder), AlgorithmValue::Null) if nullable => builder.append_null(),
            (builder, other) => {
                return Err(type_error(name, builder.expected(), other));
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Uuid(builder) => Arc::new(builder.finish()),
            Self::UuidList(builder) => Arc::new(builder.finish()),
            Self::Float32List(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
        }
    }

    fn expected(&self) -> &'static str {
        match self {
            Self::Uuid(_) => "FixedSizeBinary(16)",
            Self::UuidList(_) => "List<FixedSizeBinary(16)>",
            Self::Float32List(_) => "List<Float32>",
            Self::Utf8(_) => "Utf8",
            Self::Boolean(_) => "Boolean",
            Self::UInt64(_) => "UInt64",
            Self::Int64(_) => "Int64",
            Self::Float64(_) => "Float64",
        }
    }
}

impl AlgorithmArrowSink {
    fn flush_batch(&mut self) -> Result<(), AlgorithmError> {
        let columns = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        // Rebuild empty builders for the next window.
        self.builders = self
            .schema
            .fields
            .iter()
            .map(|field| ColumnBuilder::new(field.data_type))
            .collect::<Result<Vec<_>, _>>()?;
        let rows = self.current_rows;
        self.current_rows = 0;
        let batch = RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .map_err(|error| shape_error(error.to_string()))?;
        debug_assert_eq!(batch.num_rows(), rows);
        // Checked Arrow capacity: reject batches that already overflow i32 list offsets.
        check_batch_capacity(&batch)?;
        self.finished.push(batch);
        Ok(())
    }
}

fn coalesce_batches(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<RecordBatch, AlgorithmError> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let total_rows = batches
        .iter()
        .map(RecordBatch::num_rows)
        .try_fold(0_usize, |acc, rows| acc.checked_add(rows))
        .ok_or_else(|| shape_error("coalesced output row count overflows usize"))?;
    if total_rows > i32::MAX as usize {
        return Err(shape_error(
            "coalesced Arrow batch exceeds checked i32 row capacity",
        ));
    }
    for batch in batches {
        check_batch_capacity(batch)?;
    }
    concat_batches(schema, batches).map_err(|error| {
        shape_error(format!(
            "Arrow capacity exceeded while coalescing algorithm batches: {error}"
        ))
    })
}

fn check_batch_capacity(batch: &RecordBatch) -> Result<(), AlgorithmError> {
    if batch.num_rows() > i32::MAX as usize {
        return Err(shape_error("Arrow batch exceeds checked i32 row capacity"));
    }
    for column in batch.columns() {
        if let Some(list) = column.as_any().downcast_ref::<ListArray>() {
            if list.values().len() > i32::MAX as usize {
                return Err(shape_error(
                    "Arrow list values exceed checked i32 offset capacity",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn arrow_type(logical: AlgorithmFieldType) -> DataType {
    match logical {
        AlgorithmFieldType::Uuid => DataType::FixedSizeBinary(16),
        AlgorithmFieldType::UuidList => DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(16),
            false,
        ))),
        AlgorithmFieldType::Float32List => {
            DataType::List(Arc::new(Field::new("item", DataType::Float32, false)))
        }
        AlgorithmFieldType::Utf8 => DataType::Utf8,
        AlgorithmFieldType::Boolean => DataType::Boolean,
        AlgorithmFieldType::UInt64 => DataType::UInt64,
        AlgorithmFieldType::Int64 => DataType::Int64,
        AlgorithmFieldType::Float64 => DataType::Float64,
    }
}

pub(crate) const fn schema_version() -> &'static str {
    SCHEMA_VERSION
}

/// Decode Arrow batches back to logical rows for test assertions only.
pub(crate) fn decode_logical_rows(
    schema: &AlgorithmResultSchema,
    batch: &RecordBatch,
) -> Result<Vec<Vec<AlgorithmValue>>, AlgorithmError> {
    if batch.num_columns() != schema.fields.len() {
        return Err(shape_error("batch width does not match logical schema"));
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(schema.fields.len());
        for (column_index, field) in schema.fields.iter().enumerate() {
            values.push(decode_value(
                field.data_type,
                field.nullable,
                batch.column(column_index).as_ref(),
                row,
            )?);
        }
        rows.push(values);
    }
    Ok(rows)
}

fn decode_value(
    data_type: AlgorithmFieldType,
    nullable: bool,
    column: &dyn Array,
    row: usize,
) -> Result<AlgorithmValue, AlgorithmError> {
    if column.is_null(row) {
        if nullable {
            return Ok(AlgorithmValue::Null);
        }
        return Err(shape_error("unexpected null in non-nullable column"));
    }
    match data_type {
        AlgorithmFieldType::Uuid => {
            let array = column
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| shape_error("expected FixedSizeBinary(16)"))?;
            let bytes: [u8; 16] = array
                .value(row)
                .try_into()
                .map_err(|_| shape_error("malformed UUID width"))?;
            Ok(AlgorithmValue::Uuid(bytes))
        }
        AlgorithmFieldType::UuidList => {
            let array = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| shape_error("expected List<FixedSizeBinary(16)>"))?;
            let values = array.value(row);
            let uuids = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| shape_error("expected FixedSizeBinary(16) list items"))?;
            let mut out = Vec::with_capacity(uuids.len());
            for index in 0..uuids.len() {
                let bytes: [u8; 16] = uuids
                    .value(index)
                    .try_into()
                    .map_err(|_| shape_error("malformed UUID list item"))?;
                out.push(bytes);
            }
            Ok(AlgorithmValue::UuidList(out))
        }
        AlgorithmFieldType::Float32List => {
            let array = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| shape_error("expected List<Float32>"))?;
            let values = array.value(row);
            let floats = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| shape_error("expected Float32 list items"))?;
            Ok(AlgorithmValue::Float32List(floats.values().to_vec()))
        }
        AlgorithmFieldType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| shape_error("expected Utf8"))?;
            Ok(AlgorithmValue::Utf8(array.value(row).to_owned()))
        }
        AlgorithmFieldType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| shape_error("expected Boolean"))?;
            Ok(AlgorithmValue::Boolean(array.value(row)))
        }
        AlgorithmFieldType::UInt64 => {
            let array = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| shape_error("expected UInt64"))?;
            Ok(AlgorithmValue::UInt64(array.value(row)))
        }
        AlgorithmFieldType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| shape_error("expected Int64"))?;
            Ok(AlgorithmValue::Int64(array.value(row)))
        }
        AlgorithmFieldType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| shape_error("expected Float64"))?;
            Ok(AlgorithmValue::Float64(array.value(row)))
        }
    }
}

fn type_error(name: &str, expected: &str, actual: &AlgorithmValue) -> AlgorithmError {
    shape_error(format!(
        "field {name:?} requires {expected}, received {}",
        value_kind(actual)
    ))
}

fn value_kind(value: &AlgorithmValue) -> &'static str {
    match value {
        AlgorithmValue::Null => "Null",
        AlgorithmValue::Uuid(_) => "Uuid",
        AlgorithmValue::UuidList(_) => "UuidList",
        AlgorithmValue::Float32List(_) => "Float32List",
        AlgorithmValue::Utf8(_) => "Utf8",
        AlgorithmValue::Boolean(_) => "Boolean",
        AlgorithmValue::UInt64(_) => "UInt64",
        AlgorithmValue::Int64(_) => "Int64",
        AlgorithmValue::Float64(_) => "Float64",
    }
}

pub(crate) fn shape_error(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: format!("invalid algorithm output: {}", message.into()),
    }
}
