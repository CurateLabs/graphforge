//! Logical graph-record extraction for checkpoint diffs.
//!
//! This adapter deliberately opens the immutable generation supplied by the
//! caller and reads it through GraphForge's graph hydration/query path. It does
//! not resolve `CURRENT`, inspect the snapshot archive layout, or read any
//! knowledge/provenance participant.

use std::collections::BTreeMap;

use arrow::array::{Array, FixedSizeBinaryArray, LargeListArray, ListArray, StructArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use graphforge_core::{ApiErrorCode, GfError};
use graphforge_storage::ResolvedProjectGeneration;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{CancellationToken, GraphForge};

/// Canonical logical state of one graph object, keyed by its durable UUID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalGraphRecord {
    pub(crate) record_uuid: Uuid,
    pub(crate) fingerprint: [u8; 32],
}

/// Deterministically ordered logical nodes and edges from one generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LogicalGraphRecords {
    pub(crate) nodes: Vec<LogicalGraphRecord>,
    pub(crate) edges: Vec<LogicalGraphRecord>,
}

/// Decode the graph participant into logical node and edge records.
///
/// Fingerprints cover stable public graph values only: UUIDs, node labels,
/// relationship types and endpoints, and user properties. Storage surrogate
/// IDs, timestamps, Parquet row groups, archive paths, and batch boundaries do
/// not participate.
#[cfg(test)]
pub(crate) fn extract_logical_graph_records(
    generation: &ResolvedProjectGeneration,
    cancellation: Option<&CancellationToken>,
) -> Result<LogicalGraphRecords, GfError> {
    extract_logical_graph_records_with_mode(
        generation,
        cancellation,
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

pub(crate) fn extract_logical_graph_records_with_mode(
    generation: &ResolvedProjectGeneration,
    cancellation: Option<&CancellationToken>,
    lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
) -> Result<LogicalGraphRecords, GfError> {
    checkpoint(cancellation)?;
    let graph = GraphForge::open_resolved_with_lifecycle_mode(
        generation.container_root().to_path_buf(),
        generation.clone(),
        true,
        lifecycle_mode,
    )?;

    let nodes = streamed_logical_rows(
        &graph,
        "MATCH (n) RETURN n.node_uuid AS record_uuid, \
         labels(n) AS labels, properties(n) AS properties",
        cancellation,
    )?;

    checkpoint(cancellation)?;
    let edges = streamed_logical_rows(
        &graph,
        "MATCH (src)-[r]->(dst) RETURN r.edge_uuid AS record_uuid, \
         src.node_uuid AS source_uuid, dst.node_uuid AS target_uuid, \
         type(r) AS relationship_type, properties(r) AS properties",
        cancellation,
    )?;

    Ok(LogicalGraphRecords { nodes, edges })
}

fn streamed_logical_rows(
    graph: &GraphForge,
    query: &str,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<LogicalGraphRecord>, GfError> {
    const PAGE_ROWS: usize = 4096;
    let mut records = Vec::new();
    let mut offset = 0_usize;
    loop {
        checkpoint(cancellation)?;
        // LIMIT bounds all materialization performed by one execution stream,
        // so cancellation is observed at least every 4,096 decoded records
        // even when an executor chooses a larger physical batch size.
        let page_query = format!("{query} ORDER BY record_uuid SKIP {offset} LIMIT {PAGE_ROWS}");
        let mut stream = graph.execute_stream(&page_query)?;
        let mut page_count = 0_usize;
        loop {
            checkpoint(cancellation)?;
            let next = graph.block_on(async { Ok(stream.next().await) })?;
            let Some(batch) = next else { break };
            let batch = batch.map_err(GfError::from_execution_error)?;
            page_count += batch.num_rows();
            records.extend(logical_rows(std::slice::from_ref(&batch), cancellation)?);
        }
        if page_count < PAGE_ROWS {
            break;
        }
        offset = offset
            .checked_add(page_count)
            .ok_or_else(|| schema_error("logical graph pagination offset exceeds this platform"))?;
    }
    records.sort_by_key(|row| row.record_uuid);
    if records
        .windows(2)
        .any(|pair| pair[0].record_uuid == pair[1].record_uuid)
    {
        return Err(schema_error("logical graph UUID is duplicated"));
    }
    Ok(records)
}

fn logical_rows(
    batches: &[RecordBatch],
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<LogicalGraphRecord>, GfError> {
    let mut records = BTreeMap::new();
    let mut decoded = 0_usize;
    for batch in batches {
        let uuids = batch
            .column_by_name("record_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| schema_error("logical graph UUID column is incompatible"))?;
        for row in 0..batch.num_rows() {
            if decoded.is_multiple_of(4096) {
                checkpoint(cancellation)?;
            }
            decoded += 1;
            if uuids.is_null(row) || uuids.value_length() != 16 {
                return Err(schema_error("logical graph UUID is invalid"));
            }
            let record_uuid = Uuid::from_slice(uuids.value(row))
                .map_err(|_| schema_error("logical graph UUID is invalid"))?;
            let fingerprint = logical_fingerprint(batch, row)?;
            if records
                .insert(
                    record_uuid,
                    LogicalGraphRecord {
                        record_uuid,
                        fingerprint,
                    },
                )
                .is_some()
            {
                return Err(schema_error("logical graph UUID is duplicated"));
            }
        }
    }
    checkpoint(cancellation)?;
    Ok(records.into_values().collect())
}

fn logical_fingerprint(batch: &RecordBatch, row: usize) -> Result<[u8; 32], GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-checkpoint-logical-graph-record/1");
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let name = field.name().as_bytes();
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name);
        hash_value(&mut digest, field.data_type(), column.as_ref(), row)?;
    }
    Ok(digest.finalize().into())
}

fn hash_value(
    digest: &mut Sha256,
    data_type: &DataType,
    array: &dyn Array,
    row: usize,
) -> Result<(), GfError> {
    if array.is_null(row) {
        digest.update([0]);
        return Ok(());
    }
    digest.update([1]);
    match data_type {
        DataType::Struct(fields) => {
            digest.update(b"struct");
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| schema_error("logical graph struct is incompatible"))?;
            let mut order = (0..fields.len()).collect::<Vec<_>>();
            order.sort_by_key(|&index| fields[index].name());
            for index in order {
                let field = &fields[index];
                let name = field.name().as_bytes();
                digest.update((name.len() as u64).to_be_bytes());
                digest.update(name);
                hash_value(
                    digest,
                    field.data_type(),
                    values.column(index).as_ref(),
                    row,
                )?;
            }
        }
        DataType::List(field) => {
            digest.update(b"list");
            let values = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| schema_error("logical graph list is incompatible"))?
                .value(row);
            digest.update((values.len() as u64).to_be_bytes());
            for index in 0..values.len() {
                hash_value(digest, field.data_type(), values.as_ref(), index)?;
            }
        }
        DataType::LargeList(field) => {
            digest.update(b"large-list");
            let values = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| schema_error("logical graph large list is incompatible"))?
                .value(row);
            digest.update((values.len() as u64).to_be_bytes());
            for index in 0..values.len() {
                hash_value(digest, field.data_type(), values.as_ref(), index)?;
            }
        }
        _ => {
            digest.update(b"scalar");
            let value =
                arrow::util::display::array_value_to_string(array, row).map_err(|error| {
                    schema_error(format!("logical graph value is invalid: {error}"))
                })?;
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    Ok(())
}

fn checkpoint(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    cancellation.map_or(Ok(()), CancellationToken::checkpoint)
}

fn schema_error(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::PropValue;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn extracts_uuid_ordered_logical_nodes_and_edges_from_pinned_generation() {
        let graph = GraphForge::new(None).unwrap();
        let second = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("B".into()))]),
            )
            .unwrap();
        let first = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("A".into()))]),
            )
            .unwrap();
        let edge = graph
            .add_edge(
                &second,
                "KNOWS",
                &first,
                &HashMap::from([("since".into(), PropValue::Int(2026))]),
            )
            .unwrap();
        let generation = graph.generation_for_read().unwrap();

        let records = extract_logical_graph_records(&generation, None).unwrap();

        let mut expected_nodes = vec![first.uuid, second.uuid];
        expected_nodes.sort_unstable();
        assert_eq!(
            records
                .nodes
                .iter()
                .map(|record| record.record_uuid)
                .collect::<Vec<_>>(),
            expected_nodes
        );
        assert_eq!(records.edges.len(), 1);
        assert_eq!(records.edges[0].record_uuid, edge.uuid);
        assert!(
            records
                .nodes
                .iter()
                .chain(&records.edges)
                .all(|record| record.fingerprint != [0; 32])
        );
    }

    #[test]
    fn cancellation_is_checked_before_hydration() {
        let graph = GraphForge::new(None).unwrap();
        let generation = graph.generation_for_read().unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = extract_logical_graph_records(&generation, Some(&cancellation)).unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
    }

    #[test]
    fn fingerprint_ignores_struct_field_layout_order() {
        let uuid = Uuid::now_v7();
        let batch = |reversed: bool| {
            let mut fields: Vec<(Arc<Field>, ArrayRef)> = vec![
                (
                    Arc::new(Field::new("name", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec![Some("Alice")])) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("score", DataType::Int64, true)),
                    Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
                ),
            ];
            if reversed {
                fields.reverse();
            }
            let properties = StructArray::from(fields);
            let schema = Arc::new(Schema::new(vec![
                Field::new("record_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("properties", properties.data_type().clone(), false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(
                            [uuid.as_bytes().as_slice()].into_iter(),
                        )
                        .unwrap(),
                    ),
                    Arc::new(properties),
                ],
            )
            .unwrap()
        };

        assert_eq!(
            logical_fingerprint(&batch(false), 0).unwrap(),
            logical_fingerprint(&batch(true), 0).unwrap()
        );
    }
}
