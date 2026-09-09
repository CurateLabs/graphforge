//! Compact only the selected runtime namespace in an unpublished projection.
//! Ontology IDs, UUIDs, semantic routes and source files remain unchanged.

use super::{read_parquet, storage, string_column, validation, write_parquet};
use arrow::array::{Array, ArrayRef, ListArray, UInt32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_value::{EntityTypeId, PrimaryEntityTypeId, RuntimeEntityId};
use std::{collections::HashMap, path::Path, sync::Arc};

type EntityRemap = HashMap<RuntimeEntityId, RuntimeEntityId>;

pub(super) fn compact(catalog: &RecordBatch, target: &Path) -> Result<RecordBatch, GfError> {
    let kinds = string_column(catalog, "entry_kind")?;
    let ids = catalog
        .column_by_name("runtime_id")
        .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| validation("runtime catalog runtime_id is not UInt32"))?;
    let mut next_type = 0_u32;
    let mut next_property = 0_u32;
    let mut entities = EntityRemap::new();
    let mut mapped = Vec::with_capacity(catalog.num_rows());
    for row in 0..catalog.num_rows() {
        let counter = match kinds.value(row) {
            "entity_type" | "relation_type" => &mut next_type,
            "property" => &mut next_property,
            _ => return Err(validation("unknown runtime catalog entry kind")),
        };
        let id = *counter;
        *counter = counter
            .checked_add(1)
            .ok_or_else(|| validation("projected runtime catalog ID overflow"))?;
        if kinds.value(row) == "entity_type" {
            entities.insert(
                RuntimeEntityId::new(ids.value(row)).map_err(|e| validation(e.to_string()))?,
                RuntimeEntityId::new(id).map_err(|e| validation(e.to_string()))?,
            );
        }
        mapped.push(id);
    }
    let mut columns = catalog.columns().to_vec();
    let id_column = catalog.schema().index_of("runtime_id").map_err(storage)?;
    columns[id_column] = Arc::new(UInt32Array::from(mapped));
    let compact = RecordBatch::try_new(catalog.schema(), columns).map_err(storage)?;
    // Keep the normal persisted-catalog admission as the final namespace oracle.
    graphforge_ir::RuntimeCatalog::from_record_batch(&compact)?;
    for path in crate::mutator::node_parquet_files(target).map_err(storage)? {
        let batches = read_parquet(&path)?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| validation("projected node table lacks schema"))?;
        let rewritten = batches
            .iter()
            .map(|batch| remap_nodes(batch, &entities))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = arrow::compute::concat_batches(&schema, &rewritten).map_err(storage)?;
        write_parquet(&path, &batch)?;
    }
    Ok(compact)
}

fn entity(id: EntityTypeId, mapping: &EntityRemap) -> Result<EntityTypeId, GfError> {
    match id.tagged().runtime_entity_id() {
        None => Ok(id),
        Some(old) => mapping
            .get(&old)
            .copied()
            .map(EntityTypeId::runtime)
            .ok_or_else(|| validation("projected runtime node type has no selected catalog entry")),
    }
}

fn remap_nodes(batch: &RecordBatch, mapping: &EntityRemap) -> Result<RecordBatch, GfError> {
    let mut columns = batch.columns().to_vec();
    let schema = batch.schema();
    for (index, field) in schema.fields().iter().enumerate() {
        columns[index] = match field.name().as_str() {
            "type_id" => {
                let values = columns[index]
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| validation("node type_id is not UInt32"))?;
                let mapped = values
                    .iter()
                    .map(|value| {
                        let raw = value.ok_or_else(|| validation("node primary type is null"))?;
                        let primary = PrimaryEntityTypeId::decode(raw)
                            .map_err(|e| validation(e.to_string()))?;
                        Ok(match primary.label() {
                            None => primary.encode(),
                            Some(id) => PrimaryEntityTypeId::known(entity(id, mapping)?).encode(),
                        })
                    })
                    .collect::<Result<Vec<_>, GfError>>()?;
                Arc::new(UInt32Array::from(mapped)) as ArrayRef
            }
            "type_ids" => {
                let lists = columns[index]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| validation("node type_ids is not List"))?;
                let values = lists
                    .values()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| validation("node type_ids values are not UInt32"))?;
                let mapped = values
                    .iter()
                    .map(|value| {
                        let raw = value.ok_or_else(|| validation("node label is null"))?;
                        let id =
                            EntityTypeId::decode(raw).map_err(|e| validation(e.to_string()))?;
                        Ok(entity(id, mapping)?.encode())
                    })
                    .collect::<Result<Vec<_>, GfError>>()?;
                let DataType::List(item) = lists.data_type() else {
                    unreachable!()
                };
                Arc::new(
                    ListArray::try_new(
                        Arc::clone(item),
                        lists.offsets().clone(),
                        Arc::new(UInt32Array::from(mapped)),
                        lists.nulls().cloned(),
                    )
                    .map_err(storage)?,
                )
            }
            _ => Arc::clone(&columns[index]),
        };
    }
    RecordBatch::try_new(schema, columns).map_err(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListBuilder, UInt32Builder};
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn remap_preserves_ontology_absent_primary_and_all_runtime_labels() {
        let old = RuntimeEntityId::new(9).unwrap();
        let new = RuntimeEntityId::new(1).unwrap();
        let old_second = RuntimeEntityId::new(12).unwrap();
        let new_second = RuntimeEntityId::new(2).unwrap();
        let ontology = EntityTypeId::ontology(graphforge_core::TypeId(9)).unwrap();
        let mut labels = ListBuilder::new(UInt32Builder::new());
        for values in [
            vec![
                ontology.encode(),
                EntityTypeId::runtime(old).encode(),
                EntityTypeId::runtime(old_second).encode(),
            ],
            vec![EntityTypeId::runtime(old).encode()],
            vec![EntityTypeId::runtime(old_second).encode()],
        ] {
            labels.values().append_slice(&values);
            labels.append(true);
        }
        let labels = labels.finish();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("type_id", DataType::UInt32, false),
                Field::new("type_ids", labels.data_type().clone(), false),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![
                    ontology.encode(),
                    PrimaryEntityTypeId::absent().encode(),
                    EntityTypeId::runtime(old_second).encode(),
                ])),
                Arc::new(labels),
            ],
        )
        .unwrap();
        let mapping = HashMap::from([(old, new), (old_second, new_second)]);
        let result = remap_nodes(&batch, &mapping).unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[
                ontology.encode(),
                PrimaryEntityTypeId::absent().encode(),
                EntityTypeId::runtime(new_second).encode()
            ]
        );
        let lists = result
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let first = lists.value(0);
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[
                ontology.encode(),
                EntityTypeId::runtime(new).encode(),
                EntityTypeId::runtime(new_second).encode()
            ]
        );
        assert_eq!(
            entity(EntityTypeId::runtime(old), &mapping).unwrap(),
            EntityTypeId::runtime(new)
        );
        assert!(remap_nodes(&batch, &HashMap::from([(old, new)])).is_err());
    }
    #[test]
    fn compact_catalog_closes_type_and_property_gaps_without_changing_other_fields() {
        let mut source = graphforge_ir::RuntimeCatalog::new();
        source.intern_label("Unused").unwrap();
        source.intern_property("unused", Some("Unused")).unwrap();
        source.intern_label("Selected").unwrap();
        source.intern_relation_type("LINK").unwrap();
        source.intern_property("value", Some("Selected")).unwrap();
        source.intern_property("weight", Some("LINK")).unwrap();
        let original = source.to_record_batch();
        let indices = UInt32Array::from(vec![2, 3, 4, 5]);
        let columns = original
            .columns()
            .iter()
            .map(|array| arrow::compute::take(array.as_ref(), &indices, None).unwrap())
            .collect();
        let selected = RecordBatch::try_new(original.schema(), columns).unwrap();
        assert!(graphforge_ir::RuntimeCatalog::from_record_batch(&selected).is_err());
        let target = tempfile::tempdir().unwrap();
        let result = compact(&selected, target.path()).unwrap();
        graphforge_ir::RuntimeCatalog::from_record_batch(&result).unwrap();
        let id_index = result.schema().index_of("runtime_id").unwrap();
        assert_eq!(
            result
                .column(id_index)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[0, 1, 0, 1]
        );
        for index in 0..result.num_columns() {
            if index != id_index {
                assert_eq!(
                    result.column(index).as_ref(),
                    selected.column(index).as_ref()
                );
            }
        }
        assert_eq!(source.to_record_batch(), original);
    }
}
