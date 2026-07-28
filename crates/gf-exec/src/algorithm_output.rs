//! Stable UUID-only Arrow shaping for typed Rust algorithm output.
#![allow(
    dead_code,
    reason = "M18 shaping foundation consumed incrementally by algorithm leaves"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Builder,
    Float64Builder, Int64Builder, ListBuilder, StringBuilder, UInt32Array, UInt64Builder,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gf_core::algorithms::{Algorithm, AlgorithmFieldType};

use crate::algorithm_dispatch::{AlgorithmError, AlgorithmOutput, AlgorithmValue};

const SCHEMA_VERSION: &str = "1";

/// Append node properties to a node-oriented algorithm batch by public UUID.
///
/// Tables load once; columns are nullable, ordered by `(stem, name)`, and
/// gathered without mutating the handler-owned batch.
pub(crate) fn materialize_node_properties(
    dir: &Path,
    stems: &[String],
    batch: &RecordBatch,
) -> Result<RecordBatch, AlgorithmError> {
    materialize_node_properties_with(batch, stems, |stem| {
        gf_storage::read_properties(dir, stem)
            .map_err(|error| materialization_error(error.to_string()))
    })
}

fn materialize_node_properties_with<F>(
    batch: &RecordBatch,
    stems: &[String],
    mut load: F,
) -> Result<RecordBatch, AlgorithmError>
where
    F: FnMut(&str) -> Result<Vec<RecordBatch>, AlgorithmError>,
{
    let node_uuids = uuid_column(batch, "node_uuid", "algorithm result")?;
    if node_uuids.null_count() > 0 {
        return Err(materialization_error(
            "algorithm result contains a NULL node_uuid",
        ));
    }
    let mut ordered_stems = stems.to_vec();
    ordered_stems.sort();
    ordered_stems.dedup();

    let mut properties = BTreeMap::new();
    let mut names: HashSet<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();

    for stem in ordered_stems {
        let batches = load(&stem)?;
        if batches.is_empty() {
            continue;
        }
        let schema = batches[0].schema();
        if batches.iter().any(|candidate| candidate.schema() != schema) {
            return Err(materialization_error(format!(
                "property table {stem:?} returned inconsistent batch schemas"
            )));
        }
        let combined = concat_batches(&schema, &batches)
            .map_err(|error| materialization_error(error.to_string()))?;
        let property_uuids =
            uuid_column(&combined, "node_uuid", &format!("property table {stem:?}"))?;
        let mut row_by_uuid = HashMap::with_capacity(property_uuids.len());
        for row in 0..property_uuids.len() {
            if property_uuids.is_null(row) {
                return Err(materialization_error(format!(
                    "property table {stem:?} contains a NULL node_uuid"
                )));
            }
            let uuid = uuid_at(property_uuids, row, &stem)?;
            let row = u32::try_from(row).map_err(|_| {
                materialization_error(format!("property table {stem:?} exceeds the row limit"))
            })?;
            if row_by_uuid.insert(uuid, row).is_some() {
                return Err(materialization_error(format!(
                    "property table {stem:?} contains duplicate rows for one node_uuid"
                )));
            }
        }

        for (index, field) in schema.fields().iter().enumerate() {
            if field.name() == "node_uuid" {
                continue;
            }
            if !names.insert(field.name().clone()) {
                return Err(materialization_error(format!(
                    "property column {:?} is ambiguous across selected tables or result fields",
                    field.name()
                )));
            }
            let key = (stem.clone(), field.name().clone());
            properties.insert(
                key,
                (
                    field.clone(),
                    combined.column(index).clone(),
                    row_by_uuid.clone(),
                ),
            );
        }
    }

    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();
    for ((_stem, _name), (field, values, row_by_uuid)) in properties {
        let indices = UInt32Array::from(
            (0..node_uuids.len())
                .map(|row| {
                    if node_uuids.is_null(row) {
                        None
                    } else {
                        uuid_at(node_uuids, row, "algorithm result")
                            .ok()
                            .and_then(|uuid| row_by_uuid.get(&uuid).copied())
                    }
                })
                .collect::<Vec<_>>(),
        );
        let gathered = take(values.as_ref(), &indices, None)
            .map_err(|error| materialization_error(error.to_string()))?;
        fields.push(Arc::new(field.as_ref().clone().with_nullable(true)));
        columns.push(gathered);
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(|error| materialization_error(error.to_string()))
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    source: &str,
) -> Result<&'a FixedSizeBinaryArray, AlgorithmError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .filter(|column| column.value_length() == 16)
        .ok_or_else(|| {
            materialization_error(format!("{source} requires {name:?} as FixedSizeBinary(16)"))
        })
}

fn uuid_at(
    uuids: &FixedSizeBinaryArray,
    row: usize,
    source: &str,
) -> Result<[u8; 16], AlgorithmError> {
    uuids
        .value(row)
        .try_into()
        .map_err(|_| materialization_error(format!("{source} contains a malformed node_uuid")))
}

/// Convert typed logical handler rows into the canonical public Arrow batch.
pub(crate) fn shape_algorithm_output(
    algorithm: Algorithm,
    output: &AlgorithmOutput,
) -> Result<RecordBatch, AlgorithmError> {
    let expected = algorithm.result_schema();
    if output.schema != expected {
        return Err(shape_error(
            "handler returned a non-canonical logical schema",
        ));
    }
    for (row_index, row) in output.rows.iter().enumerate() {
        if row.len() != expected.fields.len() {
            return Err(shape_error(format!(
                "row {row_index} has {} values but schema requires {}",
                row.len(),
                expected.fields.len()
            )));
        }
    }

    let mut fields = Vec::with_capacity(expected.fields.len());
    let mut columns = Vec::with_capacity(expected.fields.len());
    for (column_index, logical) in expected.fields.iter().enumerate() {
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
        columns.push(build_column(
            logical.data_type,
            logical.nullable,
            logical.name,
            column_index,
            &output.rows,
        )?);
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
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    RecordBatch::try_new(schema, columns).map_err(|error| shape_error(error.to_string()))
}

fn arrow_type(logical: AlgorithmFieldType) -> DataType {
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

fn build_column(
    data_type: AlgorithmFieldType,
    nullable: bool,
    name: &str,
    column_index: usize,
    rows: &[Vec<AlgorithmValue>],
) -> Result<ArrayRef, AlgorithmError> {
    match data_type {
        AlgorithmFieldType::Uuid => build_uuid(nullable, name, column_index, rows),
        AlgorithmFieldType::UuidList => build_uuid_list(nullable, name, column_index, rows),
        AlgorithmFieldType::Float32List => build_float32_list(nullable, name, column_index, rows),
        AlgorithmFieldType::Utf8 => {
            let mut builder = StringBuilder::new();
            for value in column_values(rows, column_index) {
                match value {
                    AlgorithmValue::Utf8(value) => builder.append_value(value),
                    AlgorithmValue::Null if nullable => builder.append_null(),
                    other => return Err(type_error(name, "Utf8", other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        AlgorithmFieldType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for value in column_values(rows, column_index) {
                match value {
                    AlgorithmValue::Boolean(value) => builder.append_value(*value),
                    AlgorithmValue::Null if nullable => builder.append_null(),
                    other => return Err(type_error(name, "Boolean", other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        AlgorithmFieldType::UInt64 => {
            let mut builder = UInt64Builder::new();
            for value in column_values(rows, column_index) {
                match value {
                    AlgorithmValue::UInt64(value) => builder.append_value(*value),
                    AlgorithmValue::Null if nullable => builder.append_null(),
                    other => return Err(type_error(name, "UInt64", other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        AlgorithmFieldType::Int64 => {
            let mut builder = Int64Builder::new();
            for value in column_values(rows, column_index) {
                match value {
                    AlgorithmValue::Int64(value) => builder.append_value(*value),
                    AlgorithmValue::Null if nullable => builder.append_null(),
                    other => return Err(type_error(name, "Int64", other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        AlgorithmFieldType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in column_values(rows, column_index) {
                match value {
                    AlgorithmValue::Float64(value) if value.is_finite() => {
                        builder.append_value(*value);
                    }
                    AlgorithmValue::Null if nullable => builder.append_null(),
                    AlgorithmValue::Float64(_) => {
                        return Err(shape_error(format!(
                            "field {name:?} contains a non-finite Float64"
                        )));
                    }
                    other => return Err(type_error(name, "Float64", other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn build_uuid(
    nullable: bool,
    name: &str,
    column_index: usize,
    rows: &[Vec<AlgorithmValue>],
) -> Result<ArrayRef, AlgorithmError> {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in column_values(rows, column_index) {
        match value {
            AlgorithmValue::Uuid(value) => builder
                .append_value(value)
                .map_err(|error| shape_error(error.to_string()))?,
            AlgorithmValue::Null if nullable => builder.append_null(),
            other => return Err(type_error(name, "FixedSizeBinary(16)", other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_uuid_list(
    nullable: bool,
    name: &str,
    column_index: usize,
    rows: &[Vec<AlgorithmValue>],
) -> Result<ArrayRef, AlgorithmError> {
    let mut builder = ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(
        Field::new("item", DataType::FixedSizeBinary(16), false),
    ));
    for value in column_values(rows, column_index) {
        match value {
            AlgorithmValue::UuidList(values) => {
                for value in values {
                    builder
                        .values()
                        .append_value(value)
                        .map_err(|error| shape_error(error.to_string()))?;
                }
                builder.append(true);
            }
            AlgorithmValue::Null if nullable => builder.append(false),
            other => return Err(type_error(name, "List<FixedSizeBinary(16)>", other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float32_list(
    nullable: bool,
    name: &str,
    column_index: usize,
    rows: &[Vec<AlgorithmValue>],
) -> Result<ArrayRef, AlgorithmError> {
    let mut builder = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Float32,
        false,
    )));
    for value in column_values(rows, column_index) {
        match value {
            AlgorithmValue::Float32List(values) if values.iter().all(|value| value.is_finite()) => {
                for value in values {
                    builder.values().append_value(*value);
                }
                builder.append(true);
            }
            AlgorithmValue::Null if nullable => builder.append(false),
            AlgorithmValue::Float32List(_) => {
                return Err(shape_error(format!(
                    "field {name:?} contains a non-finite Float32"
                )));
            }
            other => return Err(type_error(name, "List<Float32>", other)),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn column_values(
    rows: &[Vec<AlgorithmValue>],
    column_index: usize,
) -> impl Iterator<Item = &AlgorithmValue> {
    rows.iter().map(move |row| &row[column_index])
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

fn shape_error(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: format!("invalid algorithm output: {}", message.into()),
    }
}

fn materialization_error(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: format!(
            "invalid algorithm property materialization: {}",
            message.into()
        ),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use gf_core::algorithms::{AnalyzeAlgorithm, ClusterAlgorithm, PathAlgorithm, RankAlgorithm};

    use super::*;

    const UUID: [u8; 16] = [7; 16];

    fn uuid_array(values: &[[u8; 16]]) -> FixedSizeBinaryArray {
        if values.is_empty() {
            return FixedSizeBinaryArray::new_null(16, 0);
        }
        FixedSizeBinaryArray::try_from_iter(values.iter().map(<[u8; 16]>::as_slice)).unwrap()
    }

    fn node_result(values: &[[u8; 16]]) -> RecordBatch {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("score", DataType::Float64, false),
            ],
            HashMap::from([("graphforge.algorithm".to_owned(), "degree".to_owned())]),
        ));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(uuid_array(values)),
                Arc::new(Float64Array::from(vec![1.0; values.len()])),
            ],
        )
        .unwrap()
    }

    fn property_batch(uuids: &[[u8; 16]], name: &str, values: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(name, values.data_type().clone(), values.is_nullable()),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(uuid_array(uuids)), values]).unwrap()
    }

    fn value_for(data_type: AlgorithmFieldType) -> AlgorithmValue {
        match data_type {
            AlgorithmFieldType::Uuid => AlgorithmValue::Uuid(UUID),
            AlgorithmFieldType::UuidList => AlgorithmValue::UuidList(vec![UUID]),
            AlgorithmFieldType::Float32List => AlgorithmValue::Float32List(vec![0.25, 0.75]),
            AlgorithmFieldType::Utf8 => AlgorithmValue::Utf8("partition-a".into()),
            AlgorithmFieldType::Boolean => AlgorithmValue::Boolean(true),
            AlgorithmFieldType::UInt64 => AlgorithmValue::UInt64(3),
            AlgorithmFieldType::Int64 => AlgorithmValue::Int64(4),
            AlgorithmFieldType::Float64 => AlgorithmValue::Float64(0.5),
        }
    }

    fn output(algorithm: Algorithm, populated: bool) -> AlgorithmOutput {
        let schema = algorithm.result_schema();
        AlgorithmOutput {
            schema,
            rows: populated
                .then(|| {
                    schema
                        .fields
                        .iter()
                        .map(|field| value_for(field.data_type))
                        .collect()
                })
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn every_logical_type_shapes_with_canonical_metadata() {
        let representatives = [
            Algorithm::Rank(RankAlgorithm::Degree),
            Algorithm::Cluster(ClusterAlgorithm::Components),
            Algorithm::Paths(PathAlgorithm::Bfs),
            Algorithm::Analyze(AnalyzeAlgorithm::Node2Vec),
            Algorithm::Analyze(AnalyzeAlgorithm::Conductance),
            Algorithm::Analyze(AnalyzeAlgorithm::IsDag),
            Algorithm::Analyze(AnalyzeAlgorithm::NodeColoring),
        ];
        let mut seen = std::collections::HashSet::new();
        for algorithm in representatives {
            seen.extend(
                algorithm
                    .result_schema()
                    .fields
                    .iter()
                    .map(|field| field.data_type),
            );
            let batch = shape_algorithm_output(algorithm, &output(algorithm, true)).unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm"],
                algorithm.as_str()
            );
            assert_eq!(
                batch.schema().metadata()["graphforge.verb"],
                algorithm.verb().as_str()
            );
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm_schema_version"],
                SCHEMA_VERSION
            );
        }
        assert_eq!(seen.len(), 8, "every AlgorithmFieldType is exercised");
    }

    #[test]
    fn empty_and_populated_batches_have_identical_schema() {
        for algorithm in [
            Algorithm::Rank(RankAlgorithm::Degree),
            Algorithm::Paths(PathAlgorithm::Bfs),
            Algorithm::Analyze(AnalyzeAlgorithm::Node2Vec),
        ] {
            let empty = shape_algorithm_output(algorithm, &output(algorithm, false)).unwrap();
            let populated = shape_algorithm_output(algorithm, &output(algorithm, true)).unwrap();
            assert_eq!(empty.schema(), populated.schema());
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(populated.num_rows(), 1);
        }
    }

    #[test]
    fn nullable_weight_preserves_null() {
        let algorithm = Algorithm::Analyze(AnalyzeAlgorithm::MinimumSpanningTree);
        let mut output = output(algorithm, true);
        let weight = output
            .schema
            .fields
            .iter()
            .position(|field| field.name == "weight")
            .unwrap();
        output.rows[0][weight] = AlgorithmValue::Null;
        let batch = shape_algorithm_output(algorithm, &output).unwrap();
        assert_eq!(batch.column(weight).null_count(), 1);
    }

    #[test]
    fn row_width_type_and_nullability_mismatches_are_rejected() {
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let mut wrong_width = output(algorithm, true);
        wrong_width.rows[0].pop();
        assert!(matches!(
            shape_algorithm_output(algorithm, &wrong_width),
            Err(AlgorithmError::Execution { .. })
        ));

        let mut wrong_type = output(algorithm, true);
        wrong_type.rows[0][0] = AlgorithmValue::Utf8("not-a-uuid".into());
        assert!(matches!(
            shape_algorithm_output(algorithm, &wrong_type),
            Err(AlgorithmError::Execution { .. })
        ));

        let mut null_non_nullable = output(algorithm, true);
        null_non_nullable.rows[0][0] = AlgorithmValue::Null;
        assert!(matches!(
            shape_algorithm_output(algorithm, &null_non_nullable),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn noncanonical_schema_and_nonfinite_values_are_rejected() {
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let mut wrong_schema = output(algorithm, true);
        wrong_schema.schema = Algorithm::Cluster(ClusterAlgorithm::Components).result_schema();
        assert!(matches!(
            shape_algorithm_output(algorithm, &wrong_schema),
            Err(AlgorithmError::Execution { .. })
        ));

        let mut nonfinite = output(algorithm, true);
        nonfinite.rows[0][1] = AlgorithmValue::Float64(f64::NAN);
        assert!(matches!(
            shape_algorithm_output(algorithm, &nonfinite),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn canonical_registry_never_contains_execution_surrogates() {
        for algorithm in [
            Algorithm::Rank(RankAlgorithm::Degree),
            Algorithm::Cluster(ClusterAlgorithm::Components),
            Algorithm::Paths(PathAlgorithm::Bfs),
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumSpanningTree),
        ] {
            assert!(algorithm.result_schema().fields.iter().all(|field| {
                !matches!(field.name, "node_id" | "edge_id" | "src_id" | "dst_id")
            }));
        }
    }

    #[test]
    fn properties_are_loaded_once_joined_by_uuid_and_appended_deterministically() {
        let one = [1; 16];
        let two = [2; 16];
        let missing = [3; 16];
        let input = node_result(&[two, one, missing]);
        let company = property_batch(
            &[two],
            "revenue",
            Arc::new(Int64Array::from(vec![Some(42)])),
        );
        let person = property_batch(
            &[one, two],
            "name",
            Arc::new(StringArray::from(vec![Some("Ada"), None])),
        );
        let mut reads = HashMap::<String, usize>::new();

        let result = materialize_node_properties_with(
            &input,
            &["Person".into(), "Company".into(), "Person".into()],
            |stem| {
                *reads.entry(stem.to_owned()).or_default() += 1;
                Ok(match stem {
                    "Company" => vec![company.clone()],
                    "Person" => vec![person.clone()],
                    _ => unreachable!(),
                })
            },
        )
        .unwrap();

        assert_eq!(
            reads,
            HashMap::from([("Company".into(), 1), ("Person".into(), 1)])
        );
        assert_eq!(result.schema().field(2).name(), "revenue");
        assert_eq!(result.schema().field(3).name(), "name");
        let revenue = result
            .column_by_name("revenue")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            revenue.iter().collect::<Vec<_>>(),
            vec![Some(42), None, None]
        );
        let name = result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            name.iter().collect::<Vec<_>>(),
            vec![None, Some("Ada"), None]
        );
        assert_eq!(input.num_columns(), 2, "handler output remains unchanged");
    }

    #[test]
    fn empty_and_populated_results_keep_the_same_materialized_schema() {
        let property = property_batch(
            &[[1; 16]],
            "name",
            Arc::new(StringArray::from(vec![Some("Ada")])),
        );
        let populated =
            materialize_node_properties_with(&node_result(&[[1; 16]]), &["Person".into()], |_| {
                Ok(vec![property.clone()])
            })
            .unwrap();
        let empty = materialize_node_properties_with(&node_result(&[]), &["Person".into()], |_| {
            Ok(vec![property.clone()])
        })
        .unwrap();

        assert_eq!(empty.schema(), populated.schema());
    }

    #[test]
    fn duplicate_property_rows_are_rejected() {
        let duplicate_rows = property_batch(
            &[[1; 16], [1; 16]],
            "name",
            Arc::new(StringArray::from(vec!["Ada", "Duplicate"])),
        );
        let error =
            materialize_node_properties_with(&node_result(&[[1; 16]]), &["Person".into()], |_| {
                Ok(vec![duplicate_rows.clone()])
            })
            .unwrap_err();
        assert!(error.to_string().contains("duplicate rows"));
    }
}
