//! UUID-preserving incorporation through existing native bulk construction.
use super::fields;
use crate::{CancellationToken, GfError, GraphForge, IrLiteral, OperationId};
use arrow::{
    array::{Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, StructArray},
    datatypes::Field,
    record_batch::RecordBatch,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};
use uuid::Uuid;

pub(super) fn incorporate(
    destination: &GraphForge,
    source: &GraphForge,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    incorporate_with_policy(destination, source, cancellation, true)
}

/// The Proposal owner has separately reviewed existing field updates and proves
/// dependency availability. This step only adds absent identities.
pub(crate) fn incorporate_new(
    destination: &GraphForge,
    source: &GraphForge,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    incorporate_with_policy(destination, source, cancellation, false)
}

fn incorporate_with_policy(
    destination: &GraphForge,
    source: &GraphForge,
    cancellation: &CancellationToken,
    require_identical: bool,
) -> Result<(), GfError> {
    let before = fields::read(destination, cancellation)?;
    let incoming = fields::read(source, cancellation)?;
    let mut existing = BTreeSet::new();
    for ((kind, id, field), value) in &incoming {
        if before.contains_key(&(kind.clone(), *id, "$object".into())) {
            existing.insert((kind.clone(), *id));
            if require_identical && before.get(&(kind.clone(), *id, field.clone())) != Some(value) {
                return Err(conflict());
            }
        }
    }
    for (key, _) in before
        .iter()
        .filter(|(key, _)| existing.contains(&(key.0.clone(), key.1)))
    {
        if require_identical && !incoming.contains_key(key) {
            return Err(conflict());
        }
    }
    if !graphforge_storage::uuid_membership_index_present(&destination.dir()) {
        graphforge_storage::rebuild_uuid_membership_indexes(
            &destination.dir(),
            graphforge_storage::UuidIndexBuildLimits::default(),
        )?;
    }
    let mut labels = BTreeMap::<String, Vec<IrLiteral>>::new();
    for (kind, query) in [
        (
            "node",
            "MATCH (n) RETURN n.node_uuid AS object_uuid, labels(n) AS labels, properties(n) AS properties",
        ),
        (
            "edge",
            "MATCH (s)-[r]->(t) RETURN r.edge_uuid AS object_uuid, s.node_uuid AS source_uuid, t.node_uuid AS target_uuid, type(r) AS relationship_type, properties(r) AS properties",
        ),
    ] {
        if kind == "edge" {
            apply_labels(destination, std::mem::take(&mut labels), cancellation)?;
        }
        let mut groups: Vec<Vec<RecordBatch>> = Vec::new();
        let mut bytes = 0_usize;
        crate::slices::stream_branch(source, query, cancellation, |batch| {
            let ids = batch
                .column_by_name("object_uuid")
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(invalid)?;
            for row in 0..batch.num_rows() {
                cancellation.checkpoint()?;
                let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
                if existing.contains(&(kind.into(), id)) {
                    continue;
                }
                if super::import_semantic::incorporate(
                    destination,
                    source,
                    batch,
                    row,
                    (kind, id),
                    cancellation,
                )? {
                    continue;
                }
                let input = input(batch, row, kind, id, &mut labels)?;
                bytes = bytes.saturating_add(input.get_array_memory_size());
                if bytes > 64 * 1024 * 1024 {
                    return Err(invalid());
                }
                if let Some(group) = groups.iter_mut().find(|g| g[0].schema() == input.schema()) {
                    group.push(input);
                } else {
                    groups.push(vec![input]);
                }
                if groups.len() > 256 {
                    return Err(invalid());
                }
            }
            Ok(())
        })?;
        for group in groups {
            cancellation.checkpoint()?;
            let operation = OperationId(Uuid::now_v7());
            if kind == "node" {
                destination
                    .publish_bulk_nodes(operation, &group)
                    .map_err(|e| GfError::Validation(format!("Branch node incorporation: {e}")))?;
            } else {
                destination
                    .publish_bulk_edges(operation, &group)
                    .map_err(|e| GfError::Validation(format!("Branch edge incorporation: {e}")))?;
            }
        }
    }
    Ok(())
}
fn apply_labels(
    destination: &GraphForge,
    labels: BTreeMap<String, Vec<IrLiteral>>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    for (label, ids) in labels {
        cancellation.checkpoint()?;
        let query = format!(
            "MATCH (n) WHERE n.node_uuid IN $ids SET n:`{}`",
            label.replace('`', "``")
        );
        destination.execute_with_params(
            &query,
            &HashMap::from([("ids".into(), IrLiteral::List(ids))]),
        )?;
    }
    Ok(())
}
fn input(
    batch: &RecordBatch,
    row: usize,
    kind: &str,
    id: Uuid,
    labels: &mut BTreeMap<String, Vec<IrLiteral>>,
) -> Result<RecordBatch, GfError> {
    let values = properties(
        batch
            .column_by_name("properties")
            .ok_or_else(invalid)?
            .as_ref(),
        row,
    )?;
    let property_fields = values
        .iter()
        .map(|(key, value)| Field::new(key, value.data_type().clone(), true))
        .collect();
    let mut columns = vec![
        batch
            .column_by_name("object_uuid")
            .ok_or_else(invalid)?
            .slice(row, 1),
    ];
    let schema = if kind == "node" {
        let list = batch
            .column_by_name("labels")
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(invalid)?
            .value(row);
        let names = list
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        if names.is_empty() {
            return Err(invalid());
        }
        columns.push(Arc::new(StringArray::from(vec![names.value(0)])));
        for index in 1..names.len() {
            labels
                .entry(names.value(index).into())
                .or_default()
                .push(IrLiteral::Uuid(*id.as_bytes()));
        }
        crate::bulk_node_input_schema(property_fields).map_err(|_| invalid())?
    } else {
        for name in ["relationship_type", "source_uuid", "target_uuid"] {
            columns.push(
                batch
                    .column_by_name(name)
                    .ok_or_else(invalid)?
                    .slice(row, 1),
            );
        }
        crate::bulk_edge_input_schema(property_fields).map_err(|_| invalid())?
    };
    columns.extend(values.into_values());
    RecordBatch::try_new(schema, columns).map_err(|_| invalid())
}
fn properties(array: &dyn Array, row: usize) -> Result<BTreeMap<String, ArrayRef>, GfError> {
    let value = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(invalid)?;
    if graphforge_value::heterogeneous::recognize(value.data_type())
        .map_err(|_| invalid())?
        .is_none()
    {
        return Ok(value
            .fields()
            .iter()
            .zip(value.columns())
            .map(|(f, a)| (f.name().clone(), a.slice(row, 1)))
            .collect());
    }
    let map =
        match graphforge_value::heterogeneous::decode_row(value, row).map_err(|_| invalid())? {
            graphforge_value::heterogeneous::Decoded::Null => return Ok(BTreeMap::new()),
            graphforge_value::heterogeneous::Decoded::Map(map) => map,
            graphforge_value::heterogeneous::Decoded::Payload(_) => return Err(invalid()),
        };
    let entries = map
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(invalid)?
        .value(row);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(invalid)?;
    let keys = entries
        .column_by_name(graphforge_value::heterogeneous::MAP_KEY)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    let values = entries
        .column_by_name(graphforge_value::heterogeneous::MAP_VALUE)
        .and_then(|a| a.as_any().downcast_ref::<StructArray>())
        .ok_or_else(invalid)?;
    (0..entries.len())
        .map(|i| {
            let value = match graphforge_value::heterogeneous::decode_row(values, i)
                .map_err(|_| invalid())?
            {
                graphforge_value::heterogeneous::Decoded::Null => {
                    Arc::new(arrow::array::NullArray::new(1)) as ArrayRef
                }
                graphforge_value::heterogeneous::Decoded::Payload(payload) => payload.slice(i, 1),
                graphforge_value::heterogeneous::Decoded::Map(_) => Arc::new(StructArray::from(
                    properties(values, i)?
                        .into_iter()
                        .map(|(name, a)| {
                            (Arc::new(Field::new(name, a.data_type().clone(), true)), a)
                        })
                        .collect::<Vec<_>>(),
                )),
            };
            Ok((keys.value(i).into(), value))
        })
        .collect()
}
fn invalid() -> GfError {
    GfError::Validation(
        "Branch graph incorporation exceeds supported typed shape or resource limits".into(),
    )
}
fn conflict() -> GfError {
    GfError::Project { code: graphforge_core::ProjectErrorCode::WriteConflict, message: "selected incoming object differs from the existing Branch object; resolve the exact historical conflict before incorporation".into() }
}
