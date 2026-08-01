//! Stable UUID-only Arrow shaping for graph-native search results.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, StringArray,
    new_empty_array, new_null_array,
};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_search::{FusedSearchHit, VectorLifecycleLimits, project_label_members};
use graphforge_storage::{list_property_stems, read_properties};

use super::GfError;

const SEARCH_SCHEMA_VERSION: &str = "1";
const NODE_UUID: &str = "node_uuid";
const SCORE: &str = "score";
const MATCHED_ON: &str = "matched_on";

#[derive(Clone, Copy, Debug)]
struct PropertyRow {
    batch: usize,
    row: usize,
}

#[derive(Clone, Debug)]
struct PropertySpec {
    field: Arc<Field>,
    columns: HashMap<usize, usize>,
}

type LoadedProperties = (
    Vec<RecordBatch>,
    HashMap<[u8; 16], PropertyRow>,
    BTreeMap<String, PropertySpec>,
);

/// Shape canonical graph-native search hits into one stable Arrow batch.
///
/// Property schemas are derived from current members of the required label,
/// then values are joined by UUID across primary-property routing stems. The
/// hit order is preserved exactly; retrieval owns scoring and ordering.
pub(crate) fn shape_search_output(
    project_dir: &std::path::Path,
    label_id: u32,
    hits: &[FusedSearchHit],
) -> Result<RecordBatch, GfError> {
    validate_hits(hits)?;
    let eligible = project_label_members(
        project_dir,
        label_id,
        VectorLifecycleLimits::default(),
        || Ok(()),
    )?;
    if let Some(hit) = hits.iter().find(|hit| !eligible.contains(&hit.node_uuid)) {
        return Err(storage(format!(
            "search hit UUID {:02x?} is not a current member of the requested label",
            hit.node_uuid
        )));
    }

    let (batches, rows, properties) = load_properties(project_dir, &eligible)?;
    let mut fields = Vec::<Arc<Field>>::with_capacity(properties.len() + 3);
    let mut columns = Vec::<ArrayRef>::with_capacity(properties.len() + 3);

    fields.push(Arc::new(Field::new(
        NODE_UUID,
        DataType::FixedSizeBinary(16),
        false,
    )));
    let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(hits.len(), 16);
    for hit in hits {
        uuid_builder
            .append_value(hit.node_uuid)
            .map_err(|error| execution(error.to_string()))?;
    }
    columns.push(Arc::new(uuid_builder.finish()));

    let mut ordered_properties = properties
        .into_iter()
        .map(|(name, property)| (qualified_property_name(&name), property))
        .collect::<Vec<_>>();
    ordered_properties.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut projected_names = BTreeSet::new();
    for (output_name, property) in ordered_properties {
        if !projected_names.insert(output_name.clone()) {
            return Err(storage(format!(
                "property projection produces duplicate field {output_name:?}"
            )));
        }
        fields.push(Arc::new(
            property
                .field
                .as_ref()
                .clone()
                .with_name(output_name)
                .with_nullable(true),
        ));
        columns.push(project_property(&batches, &rows, &property, hits)?);
    }

    fields.push(Arc::new(Field::new(SCORE, DataType::Float64, false)));
    columns.push(Arc::new(Float64Array::from_iter_values(
        hits.iter().map(|hit| hit.score),
    )));
    fields.push(Arc::new(Field::new(MATCHED_ON, DataType::Utf8, false)));
    columns.push(Arc::new(StringArray::from_iter_values(
        hits.iter().map(|hit| hit.matched_on.as_str()),
    )));

    let metadata = HashMap::from([
        ("graphforge.verb".to_owned(), "find".to_owned()),
        (
            "graphforge.search_schema_version".to_owned(),
            SEARCH_SCHEMA_VERSION.to_owned(),
        ),
    ]);
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns,
    )
    .map_err(|error| execution(error.to_string()))
}

fn validate_hits(hits: &[FusedSearchHit]) -> Result<(), GfError> {
    let mut seen = BTreeSet::new();
    for hit in hits {
        if !hit.score.is_finite() {
            return Err(validation("search scores must be finite"));
        }
        if !seen.insert(hit.node_uuid) {
            return Err(validation("search hit UUIDs must be unique"));
        }
    }
    Ok(())
}

fn load_properties(
    project_dir: &std::path::Path,
    eligible: &BTreeSet<[u8; 16]>,
) -> Result<LoadedProperties, GfError> {
    let mut batches = Vec::new();
    let mut rows = HashMap::new();
    let mut properties = BTreeMap::<String, PropertySpec>::new();
    for stem in list_property_stems(project_dir) {
        let stem_batches = read_properties(project_dir, &stem)
            .map_err(|error| storage(format!("read property table {stem:?}: {error}")))?;
        for batch in stem_batches {
            let batch_index = batches.len();
            let uuids = uuid_column(&batch, &stem)?;
            let mut contains_member = false;
            for row in 0..batch.num_rows() {
                if uuids.is_null(row) {
                    return Err(storage(format!(
                        "property table {stem:?} contains a NULL node_uuid"
                    )));
                }
                let node_uuid: [u8; 16] = uuids.value(row).try_into().map_err(|_| {
                    storage(format!(
                        "property table {stem:?} contains a malformed node_uuid"
                    ))
                })?;
                if !eligible.contains(&node_uuid) {
                    continue;
                }
                contains_member = true;
                if rows
                    .insert(
                        node_uuid,
                        PropertyRow {
                            batch: batch_index,
                            row,
                        },
                    )
                    .is_some()
                {
                    return Err(storage(format!(
                        "eligible UUID {node_uuid:02x?} has duplicate property rows"
                    )));
                }
            }
            if contains_member {
                register_property_schema(batch_index, &batch, &mut properties)?;
            }
            batches.push(batch);
        }
    }
    Ok((batches, rows, properties))
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    stem: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(NODE_UUID)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .filter(|column| column.value_length() == 16)
        .ok_or_else(|| {
            storage(format!(
                "property table {stem:?} requires node_uuid as FixedSizeBinary(16)"
            ))
        })
}

fn register_property_schema(
    batch_index: usize,
    batch: &RecordBatch,
    properties: &mut BTreeMap<String, PropertySpec>,
) -> Result<(), GfError> {
    for (column_index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == NODE_UUID {
            continue;
        }
        let property = properties
            .entry(field.name().clone())
            .or_insert_with(|| PropertySpec {
                field: Arc::clone(field),
                columns: HashMap::new(),
            });
        if property.field.data_type() != field.data_type()
            || property.field.metadata() != field.metadata()
        {
            return Err(storage(format!(
                "property {:?} has an inconsistent Arrow schema across routing stems",
                field.name()
            )));
        }
        property.columns.insert(batch_index, column_index);
    }
    Ok(())
}

fn project_property(
    batches: &[RecordBatch],
    rows: &HashMap<[u8; 16], PropertyRow>,
    property: &PropertySpec,
    hits: &[FusedSearchHit],
) -> Result<ArrayRef, GfError> {
    if hits.is_empty() {
        return Ok(new_empty_array(property.field.data_type()));
    }
    let pieces = hits
        .iter()
        .map(|hit| {
            rows.get(&hit.node_uuid)
                .and_then(|location| {
                    property.columns.get(&location.batch).map(|column| {
                        batches[location.batch]
                            .column(*column)
                            .slice(location.row, 1)
                    })
                })
                .unwrap_or_else(|| new_null_array(property.field.data_type(), 1))
        })
        .collect::<Vec<_>>();
    let pieces = pieces
        .iter()
        .map(std::convert::AsRef::as_ref)
        .collect::<Vec<_>>();
    concat(&pieces).map_err(|error| execution(error.to_string()))
}

fn qualified_property_name(name: &str) -> String {
    match name {
        SCORE | MATCHED_ON => format!("property.{name}"),
        _ => name.to_owned(),
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn storage(message: impl Into<String>) -> GfError {
    GfError::Storage(message.into())
}

fn execution(message: impl Into<String>) -> GfError {
    GfError::Execution(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use graphforge_core::uuid::Uuid;
    use graphforge_ir::{IrLiteral, OntologyMode, TypeId};
    use graphforge_search::MatchedOn;
    use graphforge_storage::GraphWriter;

    use super::*;

    fn uuid(marker: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = marker;
        Uuid::from_bytes(bytes)
    }

    fn hit(marker: u8, score: f64, matched_on: MatchedOn) -> FusedSearchHit {
        FusedSearchHit {
            node_uuid: *uuid(marker).as_bytes(),
            score,
            matched_on,
        }
    }

    #[test]
    fn shapes_secondary_members_properties_collisions_and_empty_schema() {
        let project = tempfile::tempdir().unwrap();
        let mut writer = GraphWriter::open_at(project.path(), OntologyMode::Strict, 1).unwrap();
        writer
            .create_node_with_labels(uuid(1), &[TypeId(1), TypeId(9)])
            .unwrap();
        writer.create_node(uuid(2), TypeId(9)).unwrap();
        writer.create_node(uuid(3), TypeId(9)).unwrap();
        writer.create_node(uuid(4), TypeId(4)).unwrap();
        writer
            .set_properties(
                &uuid(1),
                Some("Primary"),
                HashMap::from([
                    ("name".to_owned(), IrLiteral::Str("Alice".to_owned())),
                    ("score".to_owned(), IrLiteral::Int(7)),
                    ("matched_on".to_owned(), IrLiteral::Str("shadow".to_owned())),
                ]),
            )
            .unwrap();
        writer
            .set_properties(
                &uuid(2),
                Some("Secondary"),
                HashMap::from([
                    ("active".to_owned(), IrLiteral::Bool(true)),
                    ("name".to_owned(), IrLiteral::Str("Bob".to_owned())),
                ]),
            )
            .unwrap();
        writer
            .set_properties(
                &uuid(3),
                Some("Secondary"),
                HashMap::from([("name".to_owned(), IrLiteral::Str("Cara".to_owned()))]),
            )
            .unwrap();
        writer
            .set_properties(
                &uuid(4),
                Some("Other"),
                HashMap::from([(
                    "secret".to_owned(),
                    IrLiteral::Str("must not leak".to_owned()),
                )]),
            )
            .unwrap();
        writer.flush().unwrap();

        let hits = [
            hit(2, 0.75, MatchedOn::Vector),
            hit(1, 0.5, MatchedOn::TextAndVector),
            hit(3, -0.25, MatchedOn::Text),
        ];
        let batch = shape_search_output(project.path(), 9, &hits).unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "node_uuid",
                "active",
                "name",
                "property.matched_on",
                "property.score",
                "score",
                "matched_on",
            ]
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "find");
        assert_eq!(
            batch.schema().metadata()["graphforge.search_schema_version"],
            "1"
        );

        let active = batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active.value(0));
        assert!(active.is_null(1));
        assert!(active.is_null(2));
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            names.iter().collect::<Vec<_>>(),
            [Some("Bob"), Some("Alice"), Some("Cara")]
        );
        let property_scores = batch
            .column_by_name("property.score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(property_scores.is_null(0));
        assert_eq!(property_scores.value(1), 7);
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.values(), &[0.75, 0.5, -0.25]);
        let channels = batch
            .column_by_name("matched_on")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            channels.iter().collect::<Vec<_>>(),
            [Some("vector"), Some("text+vector"), Some("text")]
        );

        let empty = shape_search_output(project.path(), 9, &[]).unwrap();
        assert_eq!(empty.schema(), batch.schema());
        assert_eq!(empty.num_rows(), 0);
    }

    #[test]
    fn rejects_non_finite_duplicate_and_non_member_hits() {
        let project = tempfile::tempdir().unwrap();
        let mut writer = GraphWriter::open_at(project.path(), OntologyMode::Strict, 1).unwrap();
        writer.create_node(uuid(1), TypeId(9)).unwrap();
        writer.flush().unwrap();

        assert!(matches!(
            shape_search_output(project.path(), 9, &[hit(1, f64::NAN, MatchedOn::Text)]),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            shape_search_output(
                project.path(),
                9,
                &[hit(1, 1.0, MatchedOn::Text), hit(1, 0.5, MatchedOn::Vector),],
            ),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            shape_search_output(project.path(), 9, &[hit(2, 1.0, MatchedOn::Text)]),
            Err(GfError::Storage(_))
        ));
    }
}
