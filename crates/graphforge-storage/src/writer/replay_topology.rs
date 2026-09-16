//! Replay topology streams and their authenticated spool ownership.

use super::Arc;
use super::Array;
use super::DataType;
use super::EntityTypeId;
use super::Field;
use super::FixedSizeBinaryArray;
use super::GfError;
use super::HashMap;
use super::HashSet;
use super::Path;
use super::PrimaryEntityTypeId;
use super::REPLAY_NODE_FIXED_ROW_BYTES;
use super::RecordBatch;
use super::SchemaRef;
use super::StringArray;
use super::TOPOLOGY_NODES_SCHEMA;
use super::TYPED_EDGE_SCHEMA;
use super::TimestampMicrosecondArray;
use super::UInt32Array;
use super::UInt64Array;
use super::admit_replay_writer;
use super::fixed_uuid_array;
use super::fs;
use super::io_err;
use super::pq_err;
use super::replay_reader_reservation;
use super::replay_resource_limit;
use super::replay_writer_properties;
use super::replay_writer_reservation;
use super::size_of;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ReplayNodeSpoolEvidence {
    pub(crate) bytes: u64,
    pub(crate) allocated_bytes: u64,
}

// Only the low-budget flat-node replay strategy uses this private stream. One
// unlinked file exists at a time; it is never a graph payload or recovery input.
const REPLAY_NODE_SPOOL_LIMIT: u64 = 64 * 1024 * 1024;

struct ReplayNodeSpoolSink {
    file: fs::File,
    bytes: u64,
    limit: u64,
}

impl std::io::Write for ReplayNodeSpoolSink {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .checked_add(buffer.len() as u64)
            .is_none_or(|end| end > self.limit)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "replay node spool byte ceiling",
            ));
        }
        let written = std::io::Write::write(&mut self.file, buffer)?;
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut self.file)
    }
}

fn replay_spool_error(error: arrow::error::ArrowError) -> GfError {
    if matches!(&error, arrow::error::ArrowError::IoError(_, source) if source.kind() == std::io::ErrorKind::FileTooLarge)
    {
        replay_resource_limit("replay node spool exceeds temporary-disk byte ceiling")
    } else {
        pq_err(error)
    }
}

enum ReplayNodeInput {
    Direct(parquet::arrow::arrow_reader::ParquetRecordBatchReader),
    Spool(arrow::ipc::reader::StreamReader<fs::File>),
}

impl Iterator for ReplayNodeInput {
    type Item = Result<RecordBatch, arrow::error::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Direct(reader) => reader.next(),
            Self::Spool(reader) => reader.next(),
        }
    }
}

fn spool_replay_nodes(
    builder: parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<fs::File>,
    target: &Path,
    expected_rows: usize,
    batch_rows: usize,
    byte_limit: u64,
) -> Result<ReplayNodeInput, GfError> {
    use std::io::{Seek, SeekFrom};
    let schema = builder.schema().clone();
    let source = builder
        .with_batch_size(batch_rows)
        .build()
        .map_err(pq_err)?;
    let sink = ReplayNodeSpoolSink {
        file: tempfile::tempfile_in(target).map_err(|error| io_err(&error))?,
        bytes: 0,
        limit: byte_limit,
    };
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(sink, &schema).map_err(replay_spool_error)?;
    let mut rows = 0_usize;
    for batch in source {
        let batch = batch.map_err(pq_err)?;
        if batch.num_rows() > batch_rows || batch.schema() != schema {
            return Err(pq_err(
                "replay node spool schema or batch authority differs",
            ));
        }
        rows = rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| replay_resource_limit("replay node spool row overflow"))?;
        if rows > expected_rows {
            return Err(pq_err("replay node spool row authority differs"));
        }
        writer.write(&batch).map_err(replay_spool_error)?;
    }
    if rows != expected_rows {
        return Err(pq_err("replay node spool row authority differs"));
    }
    // The Parquet iterator and all decoder contexts have dropped before the
    // returned private IPC reader can overlap the permanent Parquet writer.
    let mut sink = writer.into_inner().map_err(replay_spool_error)?;
    sink.file
        .seek(SeekFrom::Start(0))
        .map_err(|error| io_err(&error))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(sink.file, None).map_err(pq_err)?;
    if reader.schema() != schema {
        return Err(pq_err("replay node spool schema differs"));
    }
    Ok(ReplayNodeInput::Spool(reader))
}

fn replay_node_input(
    node_path: &Path,
    target: &Path,
    node_scan: &ReplayNodeAuthority,
    overlay_bytes: usize,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    node_writer_reservation: usize,
) -> Result<(Option<ReplayNodeInput>, ReplayNodeSpoolEvidence), GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let combined = overlay_bytes
        .saturating_add(node_scan.estimated_memory())
        .saturating_add(node_scan.reader_reservation_bytes)
        .saturating_add(node_writer_reservation);
    let use_spool = node_path.exists() && combined > limits.max_replay_memory_bytes;
    let reader = if use_spool {
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(node_path).map_err(|error| io_err(&error))?,
        )
        .map_err(pq_err)?;
        let active_bytes = node_scan
            .maximum_row_bytes
            .saturating_mul(limits.max_batch_rows.min(node_scan.base_rows));
        let ipc_reservation = 64_usize
            .saturating_mul(1024)
            .saturating_add(active_bytes.saturating_mul(3))
            .saturating_add(crate::permanent_parquet::replay_schema_bytes(
                builder.schema(),
            )?);
        // Select both admitted phases before allocating the stream. No retry
        // after a writer failure, and no uncompressed permanent-output mode.
        admit_replay_writer(
            overlay_bytes,
            node_scan.estimated_memory(),
            node_scan
                .reader_reservation_bytes
                .saturating_add(ipc_reservation),
            limits.max_replay_memory_bytes,
            "topology node spool decoder",
        )?;
        admit_replay_writer(
            overlay_bytes,
            node_scan.estimated_memory(),
            node_writer_reservation.saturating_add(ipc_reservation),
            limits.max_replay_memory_bytes,
            "topology node",
        )?;
        Some(spool_replay_nodes(
            builder,
            target,
            node_scan.base_rows,
            limits.max_batch_rows,
            REPLAY_NODE_SPOOL_LIMIT,
        )?)
    } else {
        admit_replay_writer(
            overlay_bytes,
            node_scan
                .estimated_memory()
                .saturating_add(node_scan.reader_reservation_bytes),
            node_writer_reservation,
            limits.max_replay_memory_bytes,
            "topology node",
        )?;
        if node_path.exists() {
            Some(ReplayNodeInput::Direct(
                ParquetRecordBatchReaderBuilder::try_new(
                    fs::File::open(node_path).map_err(|error| io_err(&error))?,
                )
                .map_err(pq_err)?
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?,
            ))
        } else {
            None
        }
    };
    let spool_evidence = if let Some(ReplayNodeInput::Spool(reader)) = reader.as_ref() {
        ReplayNodeSpoolEvidence {
            bytes: reader
                .get_ref()
                .metadata()
                .map_err(|error| io_err(&error))?
                .len(),
            allocated_bytes: graphforge_filesystem::file_space_usage(reader.get_ref())
                .map_err(|error| io_err(&error))?
                .allocated_bytes,
        }
    } else {
        ReplayNodeSpoolEvidence::default()
    };
    Ok((reader, spool_evidence))
}

pub(super) fn stream_replay_nodes(
    source: &Path,
    target: &Path,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
) -> Result<ReplayNodeAuthority, GfError> {
    let node_path = source.join("topology/nodes.parquet");
    let output_node_path = target.join("topology/nodes.parquet");
    let mut node_scan = scan_replay_node_authority(&node_path, overlay, limits)?;
    fs::create_dir_all(output_node_path.parent().expect("node output has parent"))
        .map_err(|error| io_err(&error))?;
    let maximum_node_rows = node_scan.base_rows.saturating_add(overlay.nodes.len());
    let node_writer_reservation = replay_writer_reservation(
        TOPOLOGY_NODES_SCHEMA.as_ref(),
        maximum_node_rows,
        node_scan.maximum_row_bytes,
        limits.max_batch_rows,
    )?;
    let (reader, spool_evidence) = replay_node_input(
        &node_path,
        target,
        &node_scan,
        overlay.estimated_memory(),
        limits,
        node_writer_reservation,
    )?;
    node_scan.spool_evidence = spool_evidence;
    let output = fs::File::create(&output_node_path).map_err(|error| io_err(&error))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        output,
        TOPOLOGY_NODES_SCHEMA.clone(),
        Some(replay_writer_properties(limits.max_batch_rows)),
    )
    .map_err(pq_err)?;
    for batch in reader.into_iter().flatten() {
        let batch = batch.map_err(pq_err)?;
        if batch.num_rows() > limits.max_batch_rows {
            return Err(replay_resource_limit("graph delta replay batch rows"));
        }
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| pq_err("canonical node_uuid column is incompatible"))?;
        for row in 0..batch.num_rows() {
            let uuid = uuid::Uuid::from_slice(uuids.value(row))
                .map_err(|error| pq_err(format!("canonical node_uuid is invalid: {error}")))?
                .hyphenated()
                .to_string();
            match overlay.nodes.get(&uuid) {
                Some(Some(replacement)) => {
                    let mut replacement = replacement.clone();
                    let primary = batch
                        .column_by_name("type_id")
                        .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
                        .ok_or_else(|| pq_err("canonical type_id column is incompatible"))?;
                    if primary.is_null(row) {
                        return Err(pq_err("canonical primary type is null"));
                    }
                    replacement.primary_type =
                        PrimaryEntityTypeId::decode(primary.value(row)).map_err(pq_err)?;
                    writer
                        .write(&replay_node_batch(&[&replacement])?)
                        .map_err(pq_err)?;
                }
                Some(None) => {}
                None => writer.write(&batch.slice(row, 1)).map_err(pq_err)?,
            }
        }
    }
    let mut appended: Vec<_> = overlay
        .nodes
        .iter()
        .filter(|(uuid, row)| row.is_some() && !node_scan.existing_overlay.contains(*uuid))
        .filter_map(|(_, row)| row.as_ref())
        .collect();
    appended.sort_by_key(|row| row.node_id);
    for chunk in appended.chunks(limits.max_batch_rows) {
        writer.write(&replay_node_batch(chunk)?).map_err(pq_err)?;
        writer.flush().map_err(pq_err)?;
    }
    writer.close().map_err(pq_err)?;

    Ok(node_scan)
}

pub(super) struct ReplayNodeAuthority {
    existing_overlay: HashSet<String>,
    endpoint_ids: HashMap<String, u64>,
    deleted_nodes: HashSet<String>,
    base_rows: usize,
    maximum_row_bytes: usize,
    reader_reservation_bytes: usize,
    pub(super) spool_evidence: ReplayNodeSpoolEvidence,
}

impl ReplayNodeAuthority {
    fn estimated_memory(&self) -> usize {
        let set_bytes = |values: &HashSet<String>| {
            values.iter().fold(0_usize, |sum, value| {
                sum.saturating_add(64).saturating_add(value.len())
            })
        };
        set_bytes(&self.existing_overlay)
            .saturating_add(set_bytes(&self.deleted_nodes))
            .saturating_add(self.endpoint_ids.iter().fold(0_usize, |sum, (uuid, _)| {
                sum.saturating_add(80).saturating_add(uuid.len())
            }))
    }
}

#[allow(clippy::too_many_lines)] // One bounded authority scan validates all node invariants.
fn scan_replay_node_authority(
    node_path: &Path,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
) -> Result<ReplayNodeAuthority, GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let endpoint_uuids: HashSet<_> = overlay
        .edges
        .values()
        .filter_map(Option::as_ref)
        .flat_map(|edge| [edge.src_uuid.clone(), edge.dst_uuid.clone()])
        .collect();
    if !node_path.exists() {
        let mut endpoint_ids = HashMap::new();
        for (uuid, row) in &overlay.nodes {
            if let Some(row) = row
                && endpoint_uuids.contains(uuid)
            {
                endpoint_ids.insert(uuid.clone(), row.node_id);
            }
        }
        return Ok(ReplayNodeAuthority {
            existing_overlay: HashSet::new(),
            endpoint_ids,
            deleted_nodes: overlay
                .nodes
                .iter()
                .filter(|(_, row)| row.is_none())
                .map(|(uuid, _)| uuid.clone())
                .collect(),
            base_rows: 0,
            maximum_row_bytes: overlay
                .nodes
                .values()
                .filter_map(Option::as_ref)
                .map(|row| {
                    REPLAY_NODE_FIXED_ROW_BYTES.saturating_add(
                        row.type_ids.len().saturating_mul(size_of::<EntityTypeId>()),
                    )
                })
                .max()
                .unwrap_or(REPLAY_NODE_FIXED_ROW_BYTES),
            reader_reservation_bytes: 0,
            spool_evidence: ReplayNodeSpoolEvidence::default(),
        });
    }
    let reader_reservation = replay_reader_reservation(node_path, limits)?;
    admit_replay_writer(
        overlay.estimated_memory(),
        0,
        reader_reservation,
        limits.max_replay_memory_bytes,
        "topology node decoder",
    )?;
    let input = fs::File::open(node_path).map_err(|error| {
        GfError::Storage(format!(
            "scan canonical replay nodes at {}: {error}",
            node_path.display()
        ))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)
        .map_err(pq_err)?
        .with_batch_size(limits.max_batch_rows)
        .build()
        .map_err(pq_err)?;
    let mut existing_overlay = HashSet::new();
    let mut endpoint_ids = HashMap::new();
    let mut prior_id = 0_u64;
    let mut base_max = 0_u64;
    let mut base_rows = 0_usize;
    let mut maximum_row_bytes = REPLAY_NODE_FIXED_ROW_BYTES;
    for batch in reader {
        let batch = batch.map_err(pq_err)?;
        base_rows = base_rows.saturating_add(batch.num_rows());
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| pq_err("canonical node_uuid column is incompatible"))?;
        let ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| pq_err("canonical node_id column is incompatible"))?;
        let type_ids = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::ListArray>())
            .ok_or_else(|| pq_err("canonical type_ids column is incompatible"))?;
        let primary_types = batch
            .column_by_name("type_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| pq_err("canonical type_id column is incompatible"))?;
        for row in 0..batch.num_rows() {
            if primary_types.is_null(row) || type_ids.is_null(row) {
                return Err(pq_err("canonical node identity contains null"));
            }
            PrimaryEntityTypeId::decode(primary_types.value(row)).map_err(pq_err)?;
            let memberships = type_ids.value(row);
            let memberships = memberships
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| pq_err("canonical membership type is incompatible"))?;
            if memberships.null_count() != 0 {
                return Err(pq_err("canonical node membership contains null"));
            }
            for encoded in memberships.values() {
                EntityTypeId::decode(*encoded).map_err(pq_err)?;
            }
            let type_count = usize::try_from(type_ids.value_length(row))
                .map_err(|_| replay_resource_limit("topology node type_ids row width"))?;
            maximum_row_bytes = maximum_row_bytes.max(
                REPLAY_NODE_FIXED_ROW_BYTES
                    .saturating_add(type_count.saturating_mul(size_of::<u32>())),
            );
            let uuid = uuid::Uuid::from_slice(uuids.value(row))
                .map_err(|error| pq_err(format!("canonical node_uuid is invalid: {error}")))?
                .hyphenated()
                .to_string();
            let id = ids.value(row);
            if id <= prior_id {
                return Err(pq_err("canonical node_id order is not strictly increasing"));
            }
            prior_id = id;
            base_max = id;
            if overlay.nodes.contains_key(&uuid) {
                existing_overlay.insert(uuid.clone());
                if let Some(Some(replacement)) = overlay.nodes.get(&uuid)
                    && replacement.node_id != id
                {
                    return Err(pq_err(
                        "GF_UNSUPPORTED_PROJECT_FORMAT: node surrogate changed",
                    ));
                }
            }
            if endpoint_uuids.contains(&uuid) {
                endpoint_ids.insert(uuid, id);
            }
        }
    }
    let mut new_ids = HashSet::new();
    for (uuid, row) in &overlay.nodes {
        if let Some(row) = row
            && !existing_overlay.contains(uuid)
        {
            if row.node_id <= base_max || !new_ids.insert(row.node_id) {
                return Err(pq_err(
                    "GF_UNSUPPORTED_PROJECT_FORMAT: new node surrogate is not monotonic",
                ));
            }
            if endpoint_uuids.contains(uuid) {
                endpoint_ids.insert(uuid.clone(), row.node_id);
            }
        }
    }
    Ok(ReplayNodeAuthority {
        existing_overlay,
        endpoint_ids,
        deleted_nodes: overlay
            .nodes
            .iter()
            .filter(|(_, row)| row.is_none())
            .map(|(uuid, _)| uuid.clone())
            .collect(),
        base_rows,
        maximum_row_bytes: maximum_row_bytes.max(
            overlay
                .nodes
                .values()
                .filter_map(Option::as_ref)
                .map(|row| {
                    REPLAY_NODE_FIXED_ROW_BYTES.saturating_add(
                        row.type_ids.len().saturating_mul(size_of::<EntityTypeId>()),
                    )
                })
                .max()
                .unwrap_or(REPLAY_NODE_FIXED_ROW_BYTES),
        ),
        reader_reservation_bytes: reader_reservation,
        spool_evidence: ReplayNodeSpoolEvidence::default(),
    })
}

pub(super) fn validate_replay_edge_endpoints(
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    nodes: &ReplayNodeAuthority,
) -> Result<(), GfError> {
    for edge in overlay.edges.values().filter_map(Option::as_ref) {
        let src_id = nodes.endpoint_ids.get(&edge.src_uuid).copied();
        let dst_id = nodes.endpoint_ids.get(&edge.dst_uuid).copied();
        if src_id != Some(edge.src_id) || dst_id != Some(edge.dst_id) {
            return Err(pq_err(
                "GF_UNSUPPORTED_PROJECT_FORMAT: edge endpoint identity is missing or inconsistent",
            ));
        }
    }
    Ok(())
}

fn replay_node_batch(
    rows: &[&crate::graph_delta_journal::ReplayNodeRow],
) -> Result<RecordBatch, GfError> {
    let uuids = fixed_uuid_array(rows.iter().map(|row| row.node_uuid.as_str()))?;
    let nullable_label_sets =
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
            rows.iter()
                .map(|row| Some(row.type_ids.iter().map(|id| Some(id.encode())))),
        );
    let label_sets = arrow::array::ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        nullable_label_sets.offsets().clone(),
        nullable_label_sets.values().clone(),
        None,
    );
    RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(uuids),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.node_id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                rows.iter()
                    .map(|row| row.primary_type.encode())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(label_sets),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| row.created_at_micros)
                        .collect::<Vec<_>>(),
                )
                .with_timezone_opt(Some(Arc::from("UTC"))),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| row.updated_at_micros)
                        .collect::<Vec<_>>(),
                )
                .with_timezone_opt(Some(Arc::from("UTC"))),
            ),
        ],
    )
    .map_err(pq_err)
}

#[allow(clippy::too_many_lines)] // Two bounded passes keep validation and emission consistent.
pub(super) fn stream_replay_edges(
    target: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    route_table: &mut crate::route_component::RouteTable,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    nodes: &ReplayNodeAuthority,
) -> Result<(), GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let target_dir = target.join("topology/edges");
    fs::create_dir_all(&target_dir).map_err(|error| io_err(&error))?;
    let mut relations = inventory
        .edge_files(None)
        .into_iter()
        .map(|(route, _)| route)
        .collect::<std::collections::BTreeSet<_>>();
    let exploratory = relations.contains("_exploratory");
    relations.extend(
        overlay
            .edges
            .values()
            .filter_map(Option::as_ref)
            .map(|edge| replay_edge_physical_route(edge, inventory, exploratory).to_owned()),
    );
    let relation_authority_bytes = relations.iter().fold(0_usize, |sum, relation| {
        sum.saturating_add(64).saturating_add(relation.len())
    });
    for relation in relations {
        let source_paths = inventory.edge_files(Some(&relation));
        let component = route_table.insert(&relation, 64 * 1024 * 1024, 100_000)?;
        let target_path = target_dir.join(format!("{component}.parquet"));
        let mut existing_overlay = HashSet::new();
        let mut base_max = 0_u64;
        let mut base_rows = 0_usize;
        let mut output_schema: Option<SchemaRef> = None;
        let mut maximum_row_bytes = 128_usize;
        let expected_schema = if relation == "_exploratory" {
            crate::schemas::EXPLORATORY_EDGE_SCHEMA.clone()
        } else {
            TYPED_EDGE_SCHEMA.clone()
        };
        for (_, source_path) in &source_paths {
            let reader_reservation = replay_reader_reservation(source_path, limits)?;
            admit_replay_writer(
                overlay.estimated_memory(),
                nodes
                    .estimated_memory()
                    .saturating_add(relation_authority_bytes)
                    .saturating_add(
                        existing_overlay
                            .capacity()
                            .saturating_mul(std::mem::size_of::<&str>() + 8),
                    ),
                reader_reservation,
                limits.max_replay_memory_bytes,
                "topology edge decoder",
            )?;
            let input = fs::File::open(source_path).map_err(|error| io_err(&error))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(input).map_err(pq_err)?;
            if builder.schema().fields() != expected_schema.fields()
                || output_schema
                    .as_ref()
                    .is_some_and(|schema| schema != builder.schema())
            {
                return Err(pq_err("canonical edge route schemas differ"));
            }
            output_schema = Some(builder.schema().clone());
            let reader = builder
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?;
            for batch in reader {
                let batch = batch.map_err(pq_err)?;
                base_rows = base_rows.saturating_add(batch.num_rows());
                let uuids = required_uuid_column(&batch, "edge_uuid")?;
                let srcs = required_uuid_column(&batch, "src_uuid")?;
                let dsts = required_uuid_column(&batch, "dst_uuid")?;
                let ids = batch
                    .column_by_name("edge_id")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| pq_err("canonical edge_id column is incompatible"))?;
                for row in 0..batch.num_rows() {
                    let edge_uuid = canonical_uuid(uuids.value(row), "edge_uuid")?;
                    let src_uuid = canonical_uuid(srcs.value(row), "src_uuid")?;
                    let dst_uuid = canonical_uuid(dsts.value(row), "dst_uuid")?;
                    if !overlay.edges.contains_key(&edge_uuid)
                        && (nodes.deleted_nodes.contains(&src_uuid)
                            || nodes.deleted_nodes.contains(&dst_uuid))
                    {
                        return Err(pq_err(
                            "GF_UNSUPPORTED_PROJECT_FORMAT: retained edge references deleted node",
                        ));
                    }
                    if relation == "_exploratory" {
                        let types = batch
                            .column_by_name("rel_type_name")
                            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                            .ok_or_else(|| {
                                pq_err("canonical exploratory relationship is incompatible")
                            })?;
                        if types.is_null(row) || types.value(row).is_empty() {
                            return Err(pq_err("canonical exploratory relationship is absent"));
                        }
                        maximum_row_bytes =
                            maximum_row_bytes.max(128_usize.saturating_add(types.value(row).len()));
                    }
                    let id = ids.value(row);
                    if id <= base_max {
                        return Err(pq_err("canonical edge_id order is not strictly increasing"));
                    }
                    base_max = id;
                    if let Some((overlay_uuid, overlay_row)) =
                        overlay.edges.get_key_value(&edge_uuid)
                    {
                        existing_overlay.insert(overlay_uuid.as_str());
                        if let Some(replacement) = overlay_row
                            && replacement.edge_id != id
                        {
                            return Err(pq_err(
                                "GF_UNSUPPORTED_PROJECT_FORMAT: edge surrogate changed",
                            ));
                        }
                    }
                }
            }
        }
        let output_schema = output_schema.unwrap_or(expected_schema);
        if relation == "_exploratory" {
            maximum_row_bytes = maximum_row_bytes.max(
                overlay
                    .edges
                    .values()
                    .filter_map(Option::as_ref)
                    .map(|edge| 128_usize.saturating_add(edge.rel_type.len()))
                    .max()
                    .unwrap_or(128),
            );
        }
        let mut new_ids = HashSet::new();
        for (uuid, row) in &overlay.edges {
            if let Some(row) = row
                && replay_edge_physical_route(row, inventory, exploratory) == relation
                && !existing_overlay.contains(uuid.as_str())
                && (row.edge_id <= base_max || !new_ids.insert(row.edge_id))
            {
                return Err(pq_err(
                    "GF_UNSUPPORTED_PROJECT_FORMAT: new edge surrogate is not monotonic",
                ));
            }
        }
        let maximum_edge_rows = base_rows.saturating_add(overlay.edges.len());
        let edge_writer_reservation = replay_writer_reservation(
            output_schema.as_ref(),
            maximum_edge_rows,
            maximum_row_bytes,
            limits.max_batch_rows,
        )?;
        let route_authority_bytes = existing_overlay
            .capacity()
            .saturating_mul(std::mem::size_of::<&str>() + 8)
            .saturating_add(relation_authority_bytes)
            .saturating_add(
                source_paths
                    .iter()
                    .try_fold(0_usize, |maximum, (_, path)| {
                        Ok::<_, GfError>(maximum.max(replay_reader_reservation(path, limits)?))
                    })?,
            );
        admit_replay_writer(
            overlay.estimated_memory(),
            nodes
                .estimated_memory()
                .saturating_add(route_authority_bytes),
            edge_writer_reservation,
            limits.max_replay_memory_bytes,
            "topology edge",
        )?;
        let output = fs::File::create(&target_path).map_err(|error| io_err(&error))?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            output,
            output_schema.clone(),
            Some(replay_writer_properties(limits.max_batch_rows)),
        )
        .map_err(pq_err)?;
        for (_, source_path) in &source_paths {
            let input = fs::File::open(source_path).map_err(|error| io_err(&error))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(input)
                .map_err(pq_err)?
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?;
            for batch in reader {
                let batch = batch.map_err(pq_err)?;
                if batch.num_rows() > limits.max_batch_rows {
                    return Err(replay_resource_limit("graph delta replay batch rows"));
                }
                let uuids = batch
                    .column_by_name("edge_uuid")
                    .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| pq_err("canonical edge_uuid column is incompatible"))?;
                for row in 0..batch.num_rows() {
                    let uuid = canonical_uuid(uuids.value(row), "edge_uuid")?;
                    match overlay.edges.get(&uuid) {
                        Some(Some(replacement))
                            if replay_edge_physical_route(replacement, inventory, exploratory)
                                == relation =>
                        {
                            writer
                                .write(&replay_edge_batch_with_schema(
                                    &[replacement],
                                    &output_schema,
                                )?)
                                .map_err(pq_err)?;
                        }
                        Some(_) => {}
                        None => writer.write(&batch.slice(row, 1)).map_err(pq_err)?,
                    }
                }
            }
        }
        let mut appended: Vec<_> = overlay
            .edges
            .iter()
            .filter(|(uuid, row)| {
                row.as_ref().is_some_and(|edge| {
                    replay_edge_physical_route(edge, inventory, exploratory) == relation
                }) && !existing_overlay.contains(uuid.as_str())
            })
            .filter_map(|(_, row)| row.as_ref())
            .collect();
        appended.sort_by_key(|edge| edge.edge_id);
        for chunk in appended.chunks(limits.max_batch_rows) {
            writer
                .write(&replay_edge_batch_with_schema(chunk, &output_schema)?)
                .map_err(pq_err)?;
            writer.flush().map_err(pq_err)?;
        }
        writer.close().map_err(pq_err)?;
        for (_, path) in crate::mutator::edge_parquet_files(target, Some(&component))? {
            if path != target_path {
                fs::remove_file(&path).map_err(|error| io_err(&error))?;
            }
        }
    }
    Ok(())
}

fn required_uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| pq_err(format!("canonical {name} column is incompatible")))
}

fn canonical_uuid(bytes: &[u8], field: &str) -> Result<String, GfError> {
    uuid::Uuid::from_slice(bytes)
        .map(|value| value.hyphenated().to_string())
        .map_err(|error| pq_err(format!("canonical {field} is invalid: {error}")))
}

fn replay_edge_physical_route<'a>(
    edge: &'a crate::graph_delta_journal::ReplayEdgeRow,
    inventory: &crate::AuthenticatedPropertyInventory,
    exploratory: bool,
) -> &'a str {
    if exploratory && !inventory.has_edge_route(&edge.rel_type) {
        "_exploratory"
    } else {
        &edge.rel_type
    }
}

fn replay_edge_batch_with_schema(
    rows: &[&crate::graph_delta_journal::ReplayEdgeRow],
    schema: &SchemaRef,
) -> Result<RecordBatch, GfError> {
    let typed = replay_edge_batch(rows)?;
    let mut columns = typed.columns().to_vec();
    if schema.index_of("rel_type_name").is_ok() {
        columns.push(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|edge| edge.rel_type.as_str()),
        )));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(pq_err)
}

fn replay_edge_batch(
    rows: &[&crate::graph_delta_journal::ReplayEdgeRow],
) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(fixed_uuid_array(
                rows.iter().map(|row| row.edge_uuid.as_str()),
            )?),
            Arc::new(fixed_uuid_array(
                rows.iter().map(|row| row.src_uuid.as_str()),
            )?),
            Arc::new(fixed_uuid_array(
                rows.iter().map(|row| row.dst_uuid.as_str()),
            )?),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.edge_id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.src_id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.dst_id).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| row.created_at_micros)
                        .collect::<Vec<_>>(),
                )
                .with_timezone_opt(Some(Arc::from("UTC"))),
            ),
        ],
    )
    .map_err(pq_err)
}

#[cfg(test)]
mod tests;
