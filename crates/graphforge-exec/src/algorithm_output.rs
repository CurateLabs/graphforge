//! Stable UUID-only Arrow shaping for typed Rust algorithm output.
#![allow(
    dead_code,
    reason = "M18 shaping foundation consumed incrementally by algorithm leaves"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, AlgorithmFieldType};

use crate::algorithm_arrow_sink::{schema_version, shape_error, AlgorithmArrowSink};
use crate::algorithm_dispatch::{AlgorithmError, AlgorithmOutput, AlgorithmValue};

const SCHEMA_VERSION: &str = schema_version();

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
        // Stream property tables as bounded batches; never concat the full table (#341).
        graphforge_storage::read_properties_batched(dir, stem, 8_192)
            .map_err(|error| materialization_error(error.to_string()))
    })
}

/// Same as [`materialize_node_properties`] with an explicit property batch size.
pub(crate) fn materialize_node_properties_with_batch_size(
    dir: &Path,
    stems: &[String],
    batch: &RecordBatch,
    batch_size: usize,
) -> Result<RecordBatch, AlgorithmError> {
    materialize_node_properties_with(batch, stems, |stem| {
        graphforge_storage::read_properties_batched(dir, stem, batch_size.max(1))
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
        let property_batches = load(&stem)?;
        let property_batches: Vec<_> = property_batches
            .into_iter()
            .filter(|candidate| candidate.num_rows() > 0)
            .collect();
        if property_batches.is_empty() {
            continue;
        }
        let schema = property_batches[0].schema();
        if property_batches
            .iter()
            .any(|candidate| candidate.schema() != schema)
        {
            return Err(materialization_error(format!(
                "property table {stem:?} returned inconsistent batch schemas"
            )));
        }

        let mut row_by_uuid: HashMap<[u8; 16], (usize, u32)> =
            HashMap::with_capacity(property_batches.iter().map(RecordBatch::num_rows).sum());
        for (batch_index, property_batch) in property_batches.iter().enumerate() {
            let property_uuids = uuid_column(
                property_batch,
                "node_uuid",
                &format!("property table {stem:?}"),
            )?;
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
                if row_by_uuid.insert(uuid, (batch_index, row)).is_some() {
                    return Err(materialization_error(format!(
                        "property table {stem:?} contains duplicate rows for one node_uuid"
                    )));
                }
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
                    property_batches.clone(),
                    index,
                    row_by_uuid.clone(),
                ),
            );
        }
    }

    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();
    for ((_stem, _name), (field, property_batches, column_index, row_by_uuid)) in properties {
        let locations: Vec<Option<(usize, u32)>> = (0..node_uuids.len())
            .map(|row| {
                if node_uuids.is_null(row) {
                    None
                } else {
                    uuid_at(node_uuids, row, "algorithm result")
                        .ok()
                        .and_then(|uuid| row_by_uuid.get(&uuid).copied())
                }
            })
            .collect();
        let gathered = gather_property_column(&property_batches, column_index, &locations)?;
        fields.push(Arc::new(field.as_ref().clone().with_nullable(true)));
        columns.push(gathered);
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(|error| materialization_error(error.to_string()))
}

fn gather_property_column(
    batches: &[RecordBatch],
    column_index: usize,
    locations: &[Option<(usize, u32)>],
) -> Result<ArrayRef, AlgorithmError> {
    use arrow::compute::kernels::zip::zip;
    use arrow::array::BooleanArray;

    let mut merged: Option<ArrayRef> = None;
    for (batch_index, batch) in batches.iter().enumerate() {
        let indices = UInt32Array::from(
            locations
                .iter()
                .map(|location| match location {
                    Some((owned_batch, row)) if *owned_batch == batch_index => Some(*row),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        );
        let taken = take(batch.column(column_index).as_ref(), &indices, None)
            .map_err(|error| materialization_error(error.to_string()))?;
        merged = Some(match merged {
            None => taken,
            Some(base) => {
                let use_overlay = BooleanArray::from(
                    (0..taken.len())
                        .map(|row| Some(!taken.is_null(row)))
                        .collect::<Vec<_>>(),
                );
                zip(&use_overlay, &taken, &base)
                    .map_err(|error| materialization_error(error.to_string()))?
            }
        });
    }
    merged.ok_or_else(|| materialization_error("property gather missing batches"))
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

/// Return the canonical public Arrow batch already shaped by [`AlgorithmArrowSink`].
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
    if output.batch.schema().metadata().get("graphforge.algorithm")
        != Some(&algorithm.as_str().to_owned())
    {
        return Err(shape_error("handler batch metadata is non-canonical"));
    }
    Ok(output.batch.clone())
}

/// Shape logical rows through the shared columnar sink (tests and tiny scalar outputs).
pub(crate) fn shape_logical_rows(
    algorithm: Algorithm,
    rows: impl IntoIterator<Item = Vec<AlgorithmValue>>,
    batch_size: usize,
    output_limit: u64,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut sink = AlgorithmArrowSink::with_limits(algorithm, batch_size, output_limit)?;
    if sink.schema() != &algorithm.result_schema() {
        return Err(shape_error(
            "handler returned a non-canonical logical schema",
        ));
    }
    for row in rows {
        sink.append_row(&row)?;
    }
    sink.finish()
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
    use graphforge_core::algorithms::{
        AnalyzeAlgorithm, ClusterAlgorithm, PathAlgorithm, RankAlgorithm,
    };

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
        let rows = populated
            .then(|| {
                algorithm
                    .result_schema()
                    .fields
                    .iter()
                    .map(|field| value_for(field.data_type))
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .collect::<Vec<_>>();
        shape_logical_rows(algorithm, rows, 8_192, u64::MAX).unwrap()
    }

    fn try_shape_row(
        algorithm: Algorithm,
        row: Vec<AlgorithmValue>,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        shape_logical_rows(algorithm, [row], 8_192, u64::MAX)
    }

    fn canonical_row(algorithm: Algorithm) -> Vec<AlgorithmValue> {
        algorithm
            .result_schema()
            .fields
            .iter()
            .map(|field| value_for(field.data_type))
            .collect()
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
        let weight = algorithm
            .result_schema()
            .fields
            .iter()
            .position(|field| field.name == "weight")
            .unwrap();
        let mut row = canonical_row(algorithm);
        row[weight] = AlgorithmValue::Null;
        let shaped = try_shape_row(algorithm, row).unwrap();
        let batch = shape_algorithm_output(algorithm, &shaped).unwrap();
        assert_eq!(batch.column(weight).null_count(), 1);
    }

    #[test]
    fn row_width_type_and_nullability_mismatches_are_rejected() {
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let mut wrong_width = canonical_row(algorithm);
        wrong_width.pop();
        assert!(matches!(
            try_shape_row(algorithm, wrong_width),
            Err(AlgorithmError::Execution { .. })
        ));

        let mut wrong_type = canonical_row(algorithm);
        wrong_type[0] = AlgorithmValue::Utf8("not-a-uuid".into());
        assert!(matches!(
            try_shape_row(algorithm, wrong_type),
            Err(AlgorithmError::Execution { .. })
        ));

        let mut null_non_nullable = canonical_row(algorithm);
        null_non_nullable[0] = AlgorithmValue::Null;
        assert!(matches!(
            try_shape_row(algorithm, null_non_nullable),
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

        let mut nonfinite = canonical_row(algorithm);
        nonfinite[1] = AlgorithmValue::Float64(f64::NAN);
        assert!(matches!(
            try_shape_row(algorithm, nonfinite),
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

    #[test]
    fn property_materialization_rejects_malformed_identity_schema_and_ambiguity() {
        let wrong_result = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "node_uuid",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert_eq!(
            materialize_node_properties_with(&wrong_result, &[], |_| Ok(vec![]))
                .unwrap_err()
                .to_string(),
            "Rust algorithm execution failed: invalid algorithm property materialization: algorithm result requires \"node_uuid\" as FixedSizeBinary(16)"
        );

        let null_uuid = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            std::iter::once(None::<[u8; 16]>),
            16,
        )
        .unwrap();
        let null_result = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "node_uuid",
                DataType::FixedSizeBinary(16),
                true,
            )])),
            vec![Arc::new(null_uuid)],
        )
        .unwrap();
        assert!(
            materialize_node_properties_with(&null_result, &[], |_| Ok(vec![]))
                .unwrap_err()
                .to_string()
                .contains("NULL node_uuid")
        );

        let one = property_batch(&[[1; 16]], "name", Arc::new(StringArray::from(vec!["Ada"])));
        let incompatible = property_batch(&[[1; 16]], "age", Arc::new(Int64Array::from(vec![37])));
        assert!(
            materialize_node_properties_with(&node_result(&[[1; 16]]), &["Person".into()], |_| {
                Ok(vec![one.clone(), incompatible.clone()])
            })
            .unwrap_err()
            .to_string()
            .contains("inconsistent batch schemas")
        );

        assert!(
            materialize_node_properties_with(
                &node_result(&[[1; 16]]),
                &["Employee".into(), "Person".into()],
                |_| Ok(vec![one.clone()]),
            )
            .unwrap_err()
            .to_string()
            .contains("ambiguous")
        );

        let score = property_batch(&[[1; 16]], "score", Arc::new(Float64Array::from(vec![2.0])));
        assert!(
            materialize_node_properties_with(&node_result(&[[1; 16]]), &["Person".into()], |_| Ok(
                vec![score.clone()]
            ),)
            .unwrap_err()
            .to_string()
            .contains("ambiguous")
        );
    }

    #[test]
    fn every_output_field_enforces_its_logical_type_and_nullability() {
        for algorithm in [
            Algorithm::Rank(RankAlgorithm::Degree),
            Algorithm::Cluster(ClusterAlgorithm::Components),
            Algorithm::Paths(PathAlgorithm::Bfs),
            Algorithm::Analyze(AnalyzeAlgorithm::Node2Vec),
            Algorithm::Analyze(AnalyzeAlgorithm::Conductance),
            Algorithm::Analyze(AnalyzeAlgorithm::IsDag),
            Algorithm::Analyze(AnalyzeAlgorithm::NodeColoring),
        ] {
            let canonical = canonical_row(algorithm);
            for (index, field) in algorithm.result_schema().fields.iter().enumerate() {
                let mut wrong_type = canonical.clone();
                wrong_type[index] = AlgorithmValue::Utf8("wrong-type".into());
                if field.data_type == AlgorithmFieldType::Utf8 {
                    wrong_type[index] = AlgorithmValue::Boolean(false);
                }
                let error = try_shape_row(algorithm, wrong_type).expect_err("logical output type mismatch");
                assert!(error.to_string().contains(field.name));

                let mut null = canonical.clone();
                null[index] = AlgorithmValue::Null;
                match try_shape_row(algorithm, null) {
                    Ok(batch) => {
                        assert!(field.nullable);
                        assert_eq!(batch.record_batch().column(index).null_count(), 1);
                    }
                    Err(error) => {
                        assert!(!field.nullable);
                        assert!(error.to_string().contains(field.name));
                    }
                }
            }
        }

        for (algorithm, replacement) in [
            (
                Algorithm::Analyze(AnalyzeAlgorithm::Node2Vec),
                AlgorithmValue::Float32List(vec![f32::NAN]),
            ),
            (
                Algorithm::Analyze(AnalyzeAlgorithm::Conductance),
                AlgorithmValue::Float64(f64::INFINITY),
            ),
        ] {
            let mut malformed = canonical_row(algorithm);
            let index = malformed
                .iter()
                .zip(algorithm.result_schema().fields.iter())
                .position(|(_, field)| {
                    matches!(
                        field.data_type,
                        AlgorithmFieldType::Float32List | AlgorithmFieldType::Float64
                    )
                })
                .unwrap();
            malformed[index] = replacement;
            assert!(
                try_shape_row(algorithm, malformed)
                    .unwrap_err()
                    .to_string()
                    .contains("non-finite")
            );
        }
    }

    #[test]
    fn bounded_batches_preserve_fingerprint_and_avoid_row_dup() {
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let rows: Vec<Vec<AlgorithmValue>> = (0..17)
            .map(|index| {
                let mut uuid = [0_u8; 16];
                uuid[15] = index as u8;
                vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(f64::from(index)),
                ]
            })
            .collect();
        let small = shape_logical_rows(algorithm, rows.clone(), 4, u64::MAX).unwrap();
        let large = shape_logical_rows(algorithm, rows, 64, u64::MAX).unwrap();
        assert!(small.internal_batch_count > 1, "small batch size must roll over");
        assert_eq!(large.internal_batch_count, 1);
        assert!(
            small.peak_builder_rows <= 4,
            "peak builder rows must stay within batch_size"
        );
        assert_eq!(
            shape_algorithm_output(algorithm, &small).unwrap(),
            shape_algorithm_output(algorithm, &large).unwrap()
        );
        assert_eq!(small.rows(), large.rows());
    }

    #[test]
    fn property_enrichment_gathers_across_property_batches_without_concat() {
        let one = [1; 16];
        let two = [2; 16];
        let input = node_result(&[two, one]);
        let first = property_batch(&[two], "name", Arc::new(StringArray::from(vec![Some("Bob")])));
        let second = property_batch(&[one], "name", Arc::new(StringArray::from(vec![Some("Ada")])));
        let result = materialize_node_properties_with(&input, &["Person".into()], |_| {
            Ok(vec![first.clone(), second.clone()])
        })
        .unwrap();
        let name = result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            name.iter().collect::<Vec<_>>(),
            vec![Some("Bob"), Some("Ada")]
        );
    }
}
