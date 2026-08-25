//! [`GraphWriter`] — buffered Parquet write path for the UUID-first topology /
//! properties layout (#579).
//!
//! Callers mint UUIDv7 identifiers (via [`graphforge_core::uuid::new_v7`]) and feed
//! nodes, edges, and properties to the writer; it assigns integer surrogate IDs
//! (`node_id` / `edge_id`), buffers rows in memory, and materialises them to
//! Parquet on [`flush`](GraphWriter::flush).  The output round-trips through
//! [`GraphCatalog`](crate::GraphCatalog).
//!
//! Routing depends on [`OntologyMode`]:
//!
//! | | edges | node properties |
//! |---|---|---|
//! | Strict / Advisory | `topology/edges/TYPENAME.parquet` ([`TYPED_EDGE_SCHEMA`]) | `properties/TYPENAME.parquet` |
//! | Exploratory | `topology/edges/_exploratory.parquet` ([`EXPLORATORY_EDGE_SCHEMA`]) | `properties/_untyped.parquet` |
//!
//! Edge properties (#784) are written separately under
//! `edge_properties/REL_TYPE.parquet`, keyed by `edge_uuid` and routed by
//! relation name in **every** mode (a dedicated directory so a relation type
//! cannot collide with a node label of the same name in `properties/`).
//!
//! # Behaviour and limitations (baseline write path)
//!
//! 1. [`flush`](GraphWriter::flush) **merges** the buffered rows with whatever is
//!    already on disk (read-modify-write), so separate write sessions accumulate
//!    (#733).  Each file write is atomic (temp + rename, #790) — an I/O failure
//!    mid-write leaves the prior file intact — but the merge is **per file**: a
//!    failure between files commits some files and not others (nodes first, so
//!    the partial state is consistent; durability/fsync stays out of scope for
//!    this non-production, small-graph engine).
//!    There is no cross-session dedup: pure `CREATE` mints fresh UUIDs, so a
//!    `node_uuid` never recurs; MATCH…CREATE upsert is deferred to #703.
//! 2. Surrogate `node_id` / `edge_id` values start at 1 (0 is reserved as a
//!    sentinel) and **continue from the on-disk maximum** when a writer is opened
//!    on an existing project, so appended rows get fresh, monotonic surrogates.
//! 3. `_untyped` property schemas are inferred from the buffered literals (union
//!    of property names, type from the first non-null value seen).  A column
//!    that sees conflicting scalar types uses a tagged scalar struct so values
//!    retain their openCypher types.
//! 4. In Advisory / Strict mode the writer trusts the caller's `rel_type` as the
//!    typed edge file name — it performs no ontology validation here (that lives
//!    in the execution layer, which holds the ontology handle).
//! 5. `_untyped` property files are not auto-registered by [`GraphCatalog`] yet;
//!    only `node_uuid` is in its read schema until the runtime catalog learns
//!    the columns.
//! 6. The writer only ever creates `topology/` and `properties/` (the always-on
//!    baseline capabilities); capability-gated directories for other features
//!    are deferred to when those capabilities exist.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, Float64Array, Float64Builder,
    Int64Builder, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use graphforge_core::uuid::{Uuid, to_bytes};
use graphforge_core::{
    GfError, OntologyMode, SpatialCoordinates, SpatialCrs, SpatialGeometryType, SpatialType,
    SpatialValue, TypeId,
};
use graphforge_ir::IrLiteral;

/// Identity, labels, and properties of a buffered node matched by MERGE.
pub type PendingNodeMatch = ([u8; 16], u64, u32, Vec<u32>, HashMap<String, IrLiteral>);

use crate::schemas::{
    EXPLORATORY_EDGE_SCHEMA, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, uuid_field,
};

/// File stem for the exploratory catch-all edge file.
const EXPLORATORY_STEM: &str = "_exploratory";
/// File stem for the untyped catch-all property file.
const UNTYPED_STEM: &str = "_untyped";
/// Sentinel UUID for "no provenance" (all-zero bytes).
/// Join-key column name for node-property files.
const NODE_PROPERTY_UUID_FIELD: &str = "node_uuid";
/// Join-key column name for edge-property files.
const EDGE_PROPERTY_UUID_FIELD: &str = "edge_uuid";

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn io_err(e: &std::io::Error) -> GfError {
    GfError::Storage(e.to_string())
}

fn pq_err(e: impl fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

// ---------------------------------------------------------------------------
// Buffered rows
// ---------------------------------------------------------------------------

struct NodeRow {
    node_uuid: [u8; 16],
    node_id: u64,
    type_id: u32,
    type_ids: Vec<u32>,
}

struct EdgeRow {
    edge_uuid: [u8; 16],
    src_uuid: [u8; 16],
    dst_uuid: [u8; 16],
    edge_id: u64,
    src_id: u64,
    dst_id: u64,
    /// `Some` for the exploratory file (carries the relation name as a column);
    /// `None` for typed edge files.
    rel_type_name: Option<String>,
}

struct PropRow {
    node_uuid: [u8; 16],
    props: HashMap<String, IrLiteral>,
}

struct EdgePropRow {
    edge_uuid: [u8; 16],
    props: HashMap<String, IrLiteral>,
}

type TypedPropertyRow = (String, [u8; 16], HashMap<String, IrLiteral>);
#[cfg(test)]
type ReconstructedEdge<'a> = (&'a String, &'a (String, String, String));

/// Apply a bounded GFDR overlay while scanning the canonical base in bounded
/// Arrow batches. Only overlay identities are retained across batches.
pub(crate) fn write_replay_overlay_streaming(
    source: &Path,
    target: &Path,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
) -> Result<(), GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let node_path = source.join("topology/nodes.parquet");
    let output_node_path = target.join("topology/nodes.parquet");
    let node_scan = scan_replay_node_authority(&node_path, overlay, limits)?;
    let reader = if node_path.exists() {
        let node_file = fs::File::open(&node_path).map_err(|error| {
            GfError::Storage(format!(
                "open canonical replay nodes at {}: {error}",
                node_path.display()
            ))
        })?;
        Some(
            ParquetRecordBatchReaderBuilder::try_new(node_file)
                .map_err(pq_err)?
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?,
        )
    } else {
        None
    };
    fs::create_dir_all(output_node_path.parent().expect("node output has parent"))
        .map_err(|error| io_err(&error))?;
    let output = fs::File::create(&output_node_path).map_err(|error| io_err(&error))?;
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(output, TOPOLOGY_NODES_SCHEMA.clone(), None)
            .map_err(pq_err)?;
    for batch in reader.into_iter().flatten() {
        let batch = batch.map_err(pq_err)?;
        if batch.num_rows() > limits.max_batch_rows {
            return Err(GfError::Storage(
                "GF_RESOURCE_LIMIT: graph delta replay batch rows".into(),
            ));
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
                Some(Some(replacement)) => writer
                    .write(&replay_node_batch(&[replacement])?)
                    .map_err(pq_err)?,
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
    if !appended.is_empty() {
        writer
            .write(&replay_node_batch(&appended)?)
            .map_err(pq_err)?;
    }
    writer.close().map_err(pq_err)?;

    validate_replay_edge_endpoints(overlay, &node_scan)?;
    stream_replay_edges(source, target, overlay, limits, &node_scan)?;
    stream_replay_properties(
        source,
        target,
        overlay,
        limits,
        false,
        &node_scan.existing_overlay,
    )?;
    stream_replay_properties(source, target, overlay, limits, true, &HashSet::new())?;
    Ok(())
}

struct ReplayNodeAuthority {
    existing_overlay: HashSet<String>,
    endpoint_ids: HashMap<String, u64>,
    deleted_nodes: HashSet<String>,
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
        });
    }
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
    for batch in reader {
        let batch = batch.map_err(pq_err)?;
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| pq_err("canonical node_uuid column is incompatible"))?;
        let ids = batch
            .column_by_name("node_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| pq_err("canonical node_id column is incompatible"))?;
        for row in 0..batch.num_rows() {
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
    })
}

fn validate_replay_edge_endpoints(
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
                .map(|row| Some(row.type_ids.iter().copied().map(Some))),
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
                    .map(|row| row.type_ids.first().copied().unwrap_or(u32::MAX))
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
fn stream_replay_edges(
    source: &Path,
    target: &Path,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    nodes: &ReplayNodeAuthority,
) -> Result<(), GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let source_dir = source.join("topology/edges");
    let target_dir = target.join("topology/edges");
    fs::create_dir_all(&target_dir).map_err(|error| io_err(&error))?;
    let mut relations = std::collections::BTreeSet::new();
    if source_dir.exists() {
        for entry in fs::read_dir(&source_dir).map_err(|error| io_err(&error))? {
            let entry = entry.map_err(|error| io_err(&error))?;
            let path = entry.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                let stem = path
                    .file_stem()
                    .and_then(std::ffi::OsStr::to_str)
                    .ok_or_else(|| pq_err("edge Parquet stem is not UTF-8"))?;
                relations.insert(stem.to_owned());
            }
        }
    }
    relations.extend(
        overlay
            .edges
            .values()
            .filter_map(Option::as_ref)
            .map(|edge| edge.rel_type.clone()),
    );
    for relation in relations {
        let source_path = source_dir.join(format!("{relation}.parquet"));
        let target_path = target_dir.join(format!("{relation}.parquet"));
        let mut existing_overlay = HashSet::new();
        let mut base_max = 0_u64;
        if source_path.exists() {
            let input = fs::File::open(&source_path).map_err(|error| io_err(&error))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(input)
                .map_err(pq_err)?
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?;
            for batch in reader {
                let batch = batch.map_err(pq_err)?;
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
                    let id = ids.value(row);
                    if id <= base_max {
                        return Err(pq_err("canonical edge_id order is not strictly increasing"));
                    }
                    base_max = id;
                    if overlay.edges.contains_key(&edge_uuid) {
                        existing_overlay.insert(edge_uuid.clone());
                        if let Some(Some(replacement)) = overlay.edges.get(&edge_uuid)
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
        let mut new_ids = HashSet::new();
        for (uuid, row) in &overlay.edges {
            if let Some(row) = row
                && row.rel_type == relation
                && !existing_overlay.contains(uuid)
                && (row.edge_id <= base_max || !new_ids.insert(row.edge_id))
            {
                return Err(pq_err(
                    "GF_UNSUPPORTED_PROJECT_FORMAT: new edge surrogate is not monotonic",
                ));
            }
        }
        let output = fs::File::create(&target_path).map_err(|error| io_err(&error))?;
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(output, TYPED_EDGE_SCHEMA.clone(), None)
                .map_err(pq_err)?;
        if source_path.exists() {
            let input = fs::File::open(&source_path).map_err(|error| io_err(&error))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(input)
                .map_err(pq_err)?
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?;
            for batch in reader {
                let batch = batch.map_err(pq_err)?;
                if batch.num_rows() > limits.max_batch_rows {
                    return Err(GfError::Storage(
                        "GF_RESOURCE_LIMIT: graph delta replay batch rows".into(),
                    ));
                }
                let uuids = batch
                    .column_by_name("edge_uuid")
                    .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| pq_err("canonical edge_uuid column is incompatible"))?;
                for row in 0..batch.num_rows() {
                    let uuid = canonical_uuid(uuids.value(row), "edge_uuid")?;
                    match overlay.edges.get(&uuid) {
                        Some(Some(replacement)) if replacement.rel_type == relation => writer
                            .write(&replay_edge_batch(&[replacement])?)
                            .map_err(pq_err)?,
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
                row.as_ref().is_some_and(|edge| edge.rel_type == relation)
                    && !existing_overlay.contains(*uuid)
            })
            .filter_map(|(_, row)| row.as_ref())
            .collect();
        appended.sort_by_key(|edge| edge.edge_id);
        if !appended.is_empty() {
            writer
                .write(&replay_edge_batch(&appended)?)
                .map_err(pq_err)?;
        }
        writer.close().map_err(pq_err)?;
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

#[allow(clippy::too_many_lines)] // Node and edge property paths deliberately share one writer.
fn stream_replay_properties(
    source: &Path,
    target: &Path,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    edge: bool,
    base_overlay_entities: &HashSet<String>,
) -> Result<(), GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let (directory, join_field, metadata_key) = if edge {
        (
            "edge_properties",
            EDGE_PROPERTY_UUID_FIELD,
            "graphforge.rel_type",
        )
    } else {
        (
            "properties",
            NODE_PROPERTY_UUID_FIELD,
            "graphforge.entity_type",
        )
    };
    let operations = if edge {
        &overlay.edge_properties
    } else {
        &overlay.node_properties
    };
    let mut stems = std::collections::BTreeSet::new();
    stems.extend(operations.keys().map(|(_, stem, _)| stem.clone()));
    let source_dir = source.join(directory);
    let target_dir = target.join(directory);
    if !operations.is_empty() && source_dir.exists() {
        for entry in fs::read_dir(&source_dir).map_err(|error| io_err(&error))? {
            let path = entry.map_err(|error| io_err(&error))?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
                && let Some(stem) = path.file_stem().and_then(std::ffi::OsStr::to_str)
            {
                stems.insert(stem.to_owned());
            }
        }
    }
    fs::create_dir_all(&target_dir).map_err(|error| io_err(&error))?;
    for stem in stems {
        let source_path = source_dir.join(format!("{stem}.parquet"));
        let target_path = target_dir.join(format!("{stem}.parquet"));
        let base_schema = if source_path.exists() {
            let input = fs::File::open(&source_path).map_err(|error| io_err(&error))?;
            ParquetRecordBatchReaderBuilder::try_new(input)
                .map_err(pq_err)?
                .schema()
                .clone()
        } else {
            Arc::new(
                Schema::new(vec![uuid_field(join_field)]).with_metadata(
                    [(metadata_key.to_owned(), stem.clone())]
                        .into_iter()
                        .collect(),
                ),
            )
        };
        let schema = replay_property_schema(
            base_schema.as_ref(),
            operations
                .iter()
                .filter(|((_, operation_stem, _), _)| operation_stem == &stem),
        )?;
        let output = fs::File::create(&target_path).map_err(|error| io_err(&error))?;
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(output, Arc::new(schema.clone()), None)
                .map_err(pq_err)?;
        let mut seen = HashSet::<String>::new();
        if source_path.exists() {
            let input = fs::File::open(&source_path).map_err(|error| io_err(&error))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(input)
                .map_err(pq_err)?
                .with_batch_size(limits.max_batch_rows)
                .build()
                .map_err(pq_err)?;
            for batch in reader {
                let batch = batch.map_err(pq_err)?;
                if batch.num_rows() > limits.max_batch_rows {
                    return Err(GfError::Storage(
                        "GF_RESOURCE_LIMIT: graph delta replay batch rows".into(),
                    ));
                }
                if edge {
                    let mut rows = Vec::new();
                    decode_property_batch(&batch, join_field, |uuid, mut props| {
                        let key = uuid::Uuid::from_bytes(uuid).hyphenated().to_string();
                        if overlay.edges.get(&key).is_some_and(Option::is_none) {
                            return;
                        }
                        apply_streamed_property_ops(&key, &stem, operations, &mut props);
                        seen.insert(key);
                        rows.push(EdgePropRow {
                            edge_uuid: uuid,
                            props,
                        });
                    })?;
                    write_property_rows_with_schema(&mut writer, &schema, join_field, &rows)?;
                } else {
                    let mut rows = Vec::new();
                    decode_property_batch(&batch, join_field, |uuid, mut props| {
                        let key = uuid::Uuid::from_bytes(uuid).hyphenated().to_string();
                        if overlay.nodes.get(&key).is_some_and(Option::is_none) {
                            return;
                        }
                        apply_streamed_property_ops(&key, &stem, operations, &mut props);
                        seen.insert(key);
                        rows.push(PropRow {
                            node_uuid: uuid,
                            props,
                        });
                    })?;
                    write_property_rows_with_schema(&mut writer, &schema, join_field, &rows)?;
                }
            }
        }
        for (entity_uuid, operation_stem, _) in operations.keys() {
            if operation_stem != &stem || seen.contains(entity_uuid) {
                continue;
            }
            let mut props = HashMap::new();
            apply_streamed_property_ops(entity_uuid, &stem, operations, &mut props);
            if props.is_empty() {
                continue;
            }
            let uuid = uuid::Uuid::parse_str(entity_uuid)
                .map_err(pq_err)?
                .into_bytes();
            if edge {
                write_property_rows_with_schema(
                    &mut writer,
                    &schema,
                    join_field,
                    &[EdgePropRow {
                        edge_uuid: uuid,
                        props,
                    }],
                )?;
            } else {
                write_property_rows_with_schema(
                    &mut writer,
                    &schema,
                    join_field,
                    &[PropRow {
                        node_uuid: uuid,
                        props,
                    }],
                )?;
            }
            seen.insert(entity_uuid.clone());
        }
        if !edge && stem == "_untyped" {
            for (entity_uuid, row) in &overlay.nodes {
                if row.is_none()
                    || base_overlay_entities.contains(entity_uuid)
                    || seen.contains(entity_uuid)
                {
                    continue;
                }
                let uuid = uuid::Uuid::parse_str(entity_uuid)
                    .map_err(pq_err)?
                    .into_bytes();
                write_property_rows_with_schema(
                    &mut writer,
                    &schema,
                    join_field,
                    &[PropRow {
                        node_uuid: uuid,
                        props: HashMap::new(),
                    }],
                )?;
                seen.insert(entity_uuid.clone());
            }
        }
        writer.close().map_err(pq_err)?;
    }
    Ok(())
}

fn apply_streamed_property_ops(
    entity_uuid: &str,
    stem: &str,
    operations: &std::collections::BTreeMap<(String, String, String), Option<IrLiteral>>,
    props: &mut HashMap<String, IrLiteral>,
) {
    for ((uuid, operation_stem, key), value) in operations {
        if uuid != entity_uuid {
            continue;
        }
        if operation_stem == stem
            && let Some(value) = value
        {
            props.insert(key.clone(), value.clone());
        } else {
            props.remove(key);
        }
    }
}

fn replay_property_schema<'a>(
    base: &Schema,
    operations: impl Iterator<Item = (&'a (String, String, String), &'a Option<IrLiteral>)>,
) -> Result<Schema, GfError> {
    let mut fields: Vec<Field> = base
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    for ((_, _, key), value) in operations {
        let Some(value) = value else { continue };
        reject_map_property_value(key, value)?;
        let Some(value_type) = ColType::of(value) else {
            continue;
        };
        if let Some(existing) = fields.iter().find(|field| field.name() == key) {
            let existing_type = col_type_from_data_type(existing.data_type())
                .ok_or_else(|| pq_err(format!("unsupported canonical property type for {key}")))?;
            if existing_type != value_type
                && !(existing_type == ColType::HetScalar && value_type.is_scalar())
            {
                return Err(pq_err(format!(
                    "GF_UNSUPPORTED_PROJECT_FORMAT: property {key} changes canonical type"
                )));
            }
        } else {
            fields.push(Field::new(key, value_type.data_type(), true));
        }
    }
    Ok(Schema::new(fields).with_metadata(base.metadata().clone()))
}

fn col_type_from_data_type(data_type: &DataType) -> Option<ColType> {
    match data_type {
        DataType::Int64 => Some(ColType::Int),
        DataType::Float64 => Some(ColType::Float),
        DataType::Boolean => Some(ColType::Bool),
        DataType::Utf8 => Some(ColType::Str),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(ColType::DateTime),
        DataType::Time64(TimeUnit::Nanosecond) => Some(ColType::Time),
        DataType::List(field) => {
            col_type_from_data_type(field.data_type()).map(|inner| ColType::List(Box::new(inner)))
        }
        DataType::Struct(fields) if fields.iter().any(|field| field.name() == "__het_int") => {
            Some(ColType::HetScalar)
        }
        DataType::Struct(fields) if fields.iter().any(|field| field.name() == "months") => {
            Some(ColType::Duration)
        }
        DataType::Struct(fields) if fields.iter().any(|field| field.name() == "epoch_day") => {
            Some(ColType::Date)
        }
        DataType::Struct(fields) if fields.iter().any(|field| field.name() == "offset_seconds") => {
            Some(ColType::ZonedTime)
        }
        DataType::Struct(fields) if fields.iter().any(|field| field.name() == "timezone") => {
            Some(ColType::ZonedDateTime)
        }
        DataType::Struct(fields) if fields.iter().any(|field| field.name() == "date") => {
            Some(ColType::LocalDateTime)
        }
        _ => None,
    }
}

fn write_property_rows_with_schema<R: PropRowLike>(
    writer: &mut parquet::arrow::ArrowWriter<fs::File>,
    schema: &Schema,
    uuid_field_name: &str,
    rows: &[R],
) -> Result<(), GfError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(
        FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| row.uuid_bytes().to_vec()))
            .map_err(pq_err)?,
    ));
    for field in schema.fields().iter().skip(1) {
        let column_type = col_type_from_data_type(field.data_type()).ok_or_else(|| {
            pq_err(format!(
                "unsupported canonical property type for {}",
                field.name()
            ))
        })?;
        columns.push(build_property_array(field.name(), column_type, rows));
    }
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).map_err(pq_err)?;
    if batch.schema().field(0).name() != uuid_field_name {
        return Err(pq_err("canonical property UUID field mismatch"));
    }
    writer.write(&batch).map_err(pq_err)
}

/// Materialize a verified base-plus-GFDR logical state as canonical Parquet in
/// a private read workspace. The committed generation remains unchanged.
#[allow(clippy::too_many_lines)] // One canonical writer keeps topology and properties atomic.
#[cfg(test)]
pub(crate) fn write_reconstructed_graph(
    dir: &Path,
    state: &crate::graph_delta_journal::ReconstructedGraphState,
) -> Result<(), GfError> {
    let topology = dir.join("topology");
    let edges_dir = topology.join("edges");
    for path in [
        topology.join("nodes.parquet"),
        edges_dir.clone(),
        dir.join("properties"),
        dir.join("edge_properties"),
    ] {
        if path.is_dir() {
            fs::remove_dir_all(&path).map_err(|error| io_err(&error))?;
        } else if path.exists() {
            fs::remove_file(&path).map_err(|error| io_err(&error))?;
        }
    }
    fs::create_dir_all(&edges_dir).map_err(|error| io_err(&error))?;

    let mut nodes: Vec<_> = state.nodes.iter().collect();
    nodes.sort_by_key(|(uuid, _)| state.node_ids.get(*uuid).copied().unwrap_or(u64::MAX));
    let uuids = fixed_uuid_array(nodes.iter().map(|(uuid, _)| uuid.as_str()))?;
    let node_ids = UInt64Array::from(
        nodes
            .iter()
            .map(|(uuid, _)| {
                state.node_ids.get(*uuid).copied().ok_or_else(|| {
                    pq_err(format!(
                        "reconstructed node {uuid} is missing its surrogate id"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let primary_types = UInt32Array::from(
        nodes
            .iter()
            .map(|(_, labels)| labels.first().copied().unwrap_or(u32::MAX))
            .collect::<Vec<_>>(),
    );
    let nullable_label_sets =
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
            nodes
                .iter()
                .map(|(_, labels)| Some(labels.iter().copied().map(Some))),
        );
    let label_sets = arrow::array::ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        nullable_label_sets.offsets().clone(),
        nullable_label_sets.values().clone(),
        None,
    );
    let node_timestamps = nodes
        .iter()
        .map(|(uuid, _)| {
            state.node_timestamps.get(*uuid).copied().ok_or_else(|| {
                pq_err(format!(
                    "reconstructed node {uuid} is missing its timestamps"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let created = TimestampMicrosecondArray::from(
        node_timestamps
            .iter()
            .map(|timestamps| timestamps.0)
            .collect::<Vec<_>>(),
    )
    .with_timezone_opt(Some(Arc::from("UTC")));
    let updated = TimestampMicrosecondArray::from(
        node_timestamps
            .iter()
            .map(|timestamps| timestamps.1)
            .collect::<Vec<_>>(),
    )
    .with_timezone_opt(Some(Arc::from("UTC")));
    let node_batch = RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(uuids),
            Arc::new(node_ids),
            Arc::new(primary_types),
            Arc::new(label_sets),
            Arc::new(created),
            Arc::new(updated),
        ],
    )
    .map_err(pq_err)?;
    write_parquet_batch(&topology.join("nodes.parquet"), &node_batch)?;

    let mut by_relation: std::collections::BTreeMap<&str, Vec<ReconstructedEdge<'_>>> =
        std::collections::BTreeMap::new();
    for (edge_uuid, edge) in &state.edges {
        by_relation
            .entry(&edge.2)
            .or_default()
            .push((edge_uuid, edge));
    }
    for (relation, mut edges) in by_relation {
        edges.sort_by_key(|(uuid, _)| state.edge_ids.get(*uuid).map_or(u64::MAX, |ids| ids.0));
        let edge_uuids = fixed_uuid_array(edges.iter().map(|(uuid, _)| uuid.as_str()))?;
        let src_uuids = fixed_uuid_array(edges.iter().map(|(_, edge)| edge.0.as_str()))?;
        let dst_uuids = fixed_uuid_array(edges.iter().map(|(_, edge)| edge.1.as_str()))?;
        let ids: Vec<_> = edges
            .iter()
            .map(|(uuid, _)| {
                state.edge_ids.get(*uuid).copied().ok_or_else(|| {
                    pq_err(format!(
                        "reconstructed edge {uuid} is missing its surrogate ids"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let created = TimestampMicrosecondArray::from(
            edges
                .iter()
                .map(|(uuid, _)| {
                    state.edge_created_at.get(*uuid).copied().ok_or_else(|| {
                        pq_err(format!(
                            "reconstructed edge {uuid} is missing its timestamp"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .with_timezone_opt(Some(Arc::from("UTC")));
        let batch = RecordBatch::try_new(
            TYPED_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(edge_uuids),
                Arc::new(src_uuids),
                Arc::new(dst_uuids),
                Arc::new(UInt64Array::from(
                    ids.iter().map(|ids| ids.0).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    ids.iter().map(|ids| ids.1).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    ids.iter().map(|ids| ids.2).collect::<Vec<_>>(),
                )),
                Arc::new(created),
            ],
        )
        .map_err(pq_err)?;
        write_parquet_batch(&edges_dir.join(format!("{relation}.parquet")), &batch)?;
    }

    let mut property_writer = GraphWriter::open_at(dir, OntologyMode::Strict, 0)?;
    for ((uuid, key), encoded) in &state.node_properties {
        let uuid = uuid::Uuid::parse_str(uuid).map_err(pq_err)?;
        let value: IrLiteral = serde_json::from_str(encoded).map_err(pq_err)?;
        let stem = state
            .node_property_stems
            .get(&(uuid.hyphenated().to_string(), key.clone()))
            .ok_or_else(|| pq_err("reconstructed node property is missing its routing stem"))?;
        property_writer.set_properties(&uuid, Some(stem), HashMap::from([(key.clone(), value)]))?;
    }
    for ((uuid, key), encoded) in &state.edge_properties {
        let uuid = uuid::Uuid::parse_str(uuid).map_err(pq_err)?;
        let value: IrLiteral = serde_json::from_str(encoded).map_err(pq_err)?;
        let relation = state
            .edge_property_stems
            .get(&(uuid.hyphenated().to_string(), key.clone()))
            .ok_or_else(|| pq_err("reconstructed edge property is missing its routing stem"))?;
        property_writer.set_edge_properties(
            &uuid,
            Some(relation),
            HashMap::from([(key.clone(), value)]),
        )?;
    }
    property_writer.flush()
}

fn fixed_uuid_array<'a>(
    values: impl Iterator<Item = &'a str>,
) -> Result<FixedSizeBinaryArray, GfError> {
    let values = values
        .map(|value| {
            uuid::Uuid::parse_str(value)
                .map(|uuid| uuid.into_bytes().to_vec())
                .map_err(pq_err)
        })
        .collect::<Result<Vec<_>, _>>()?;
    FixedSizeBinaryArray::try_from_iter(values.into_iter()).map_err(pq_err)
}

#[cfg(test)]
fn write_parquet_batch(path: &Path, batch: &RecordBatch) -> Result<(), GfError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| io_err(&error))?;
    }
    let file = fs::File::create(path).map_err(|error| io_err(&error))?;
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).map_err(pq_err)?;
    writer.write(batch).map_err(pq_err)?;
    writer.close().map_err(pq_err)?;
    Ok(())
}

/// Shared accessor over a buffered property row so the dynamic-schema inference
/// (column ordering + type coercion) works identically for node properties
/// (keyed by `node_uuid`) and edge properties (keyed by `edge_uuid`).
///
/// `props_mut` + `from_parts` additionally let the in-place SET/REMOVE rewrite
/// (#791) mutate decoded rows and mint a fresh row for an entity that had no
/// property file row yet — generically across both row kinds.
trait PropRowLike {
    fn uuid_bytes(&self) -> &[u8; 16];
    fn props(&self) -> &HashMap<String, IrLiteral>;
    fn props_mut(&mut self) -> &mut HashMap<String, IrLiteral>;
    fn from_parts(uuid: [u8; 16], props: HashMap<String, IrLiteral>) -> Self;
}

impl PropRowLike for PropRow {
    fn uuid_bytes(&self) -> &[u8; 16] {
        &self.node_uuid
    }
    fn props(&self) -> &HashMap<String, IrLiteral> {
        &self.props
    }
    fn props_mut(&mut self) -> &mut HashMap<String, IrLiteral> {
        &mut self.props
    }
    fn from_parts(uuid: [u8; 16], props: HashMap<String, IrLiteral>) -> Self {
        Self {
            node_uuid: uuid,
            props,
        }
    }
}

impl PropRowLike for EdgePropRow {
    fn uuid_bytes(&self) -> &[u8; 16] {
        &self.edge_uuid
    }
    fn props(&self) -> &HashMap<String, IrLiteral> {
        &self.props
    }
    fn props_mut(&mut self) -> &mut HashMap<String, IrLiteral> {
        &mut self.props
    }
    fn from_parts(uuid: [u8; 16], props: HashMap<String, IrLiteral>) -> Self {
        Self {
            edge_uuid: uuid,
            props,
        }
    }
}

// ---------------------------------------------------------------------------
// GraphWriter
// ---------------------------------------------------------------------------

/// Buffered Parquet writer for graph topology and properties.
///
/// See the [module docs](self) for routing rules and limitations.
pub struct GraphWriter {
    dir: PathBuf,
    mode: OntologyMode,
    /// One timestamp captured at open time, reused for every row's
    /// `created_at` / `updated_at` (microseconds since the Unix epoch, UTC).
    now_micros: i64,
    next_node_id: u64,
    next_edge_id: u64,
    /// Maps every `create_node` UUID to its surrogate so edges can resolve
    /// `src_id` / `dst_id`.
    uuid_to_node_id: HashMap<[u8; 16], u64>,
    nodes: Vec<NodeRow>,
    /// Keyed by edge file stem (`TYPENAME` or `_exploratory`).
    edges: HashMap<String, Vec<EdgeRow>>,
    /// Keyed by property file stem (`TYPENAME` or `_untyped`).
    properties: HashMap<String, Vec<PropRow>>,
    /// Edge properties, keyed by relation-type file stem (the rel name, e.g.
    /// `KNOWS`), written under `edge_properties/<stem>.parquet`.
    edge_properties: HashMap<String, Vec<EdgePropRow>>,
    /// Edges created since the last commit, captured during `flush_edges` for
    /// the adjacency delta segment (#765). Drained by `flush`/`take_pending_delta`.
    pending_delta: Vec<crate::adjacency_delta::DeltaEdge>,
    semantic_composition_fingerprint: Option<String>,
}

impl GraphWriter {
    /// Open (creating if necessary) a project directory for writing.
    ///
    /// `mode` controls edge / property routing.  The current wall-clock time is
    /// captured once and reused for all row timestamps.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the directory cannot be created.
    pub fn open(dir: &Path, mode: OntologyMode) -> Result<Self, GfError> {
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
        Self::open_at(dir, mode, now_micros)
    }

    /// Like [`open`](Self::open) but with an injected timestamp (microseconds
    /// since the Unix epoch).  Used by tests for deterministic output.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the directory cannot be created.
    pub fn open_at(dir: &Path, mode: OntologyMode, now_micros: i64) -> Result<Self, GfError> {
        fs::create_dir_all(dir).map_err(|e| io_err(&e))?;
        // Continue surrogate assignment from the on-disk maximum so a writer
        // opened on an existing project appends rather than colliding with /
        // overwriting prior rows. Absent files → max 0 → start at 1.
        let max_node_id = crate::catalog::max_node_id(dir).map_err(pq_err)?;
        let max_edge_id = crate::catalog::max_edge_id(dir).map_err(pq_err)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            mode,
            now_micros,
            next_node_id: max_node_id + 1,
            next_edge_id: max_edge_id + 1,
            uuid_to_node_id: HashMap::new(),
            nodes: Vec::new(),
            edges: HashMap::new(),
            properties: HashMap::new(),
            edge_properties: HashMap::new(),
            pending_delta: Vec::new(),
            semantic_composition_fingerprint: None,
        })
    }

    /// Attach the exact composition fingerprint used to authenticate opaque
    /// semantic routes written by this writer.
    #[must_use]
    pub fn with_semantic_composition_fingerprint(mut self, fingerprint: Option<String>) -> Self {
        self.semantic_composition_fingerprint = fingerprint;
        self
    }

    /// Buffer a new node and return its assigned `node_id` surrogate.
    ///
    /// # Errors
    /// Currently infallible; returns `Result` for forward compatibility.
    pub fn create_node(&mut self, node_uuid: Uuid, type_id: TypeId) -> Result<u64, GfError> {
        self.create_node_with_labels(node_uuid, &[type_id])
    }

    /// Buffer a node with its complete label set.
    ///
    /// The first label is the immutable primary label used for legacy property
    /// file routing. Label membership and `labels()` semantics use the complete
    /// set. An empty slice creates an unlabelled node.
    pub fn create_node_with_labels(
        &mut self,
        node_uuid: Uuid,
        type_ids: &[TypeId],
    ) -> Result<u64, GfError> {
        let bytes = to_bytes(&node_uuid);
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        // Last-writer-wins on duplicate UUID (no dedup detection at this layer).
        self.uuid_to_node_id.insert(bytes, node_id);
        self.nodes.push(NodeRow {
            node_uuid: bytes,
            node_id,
            type_id: type_ids.first().map_or(u32::MAX, |id| id.0),
            type_ids: type_ids.iter().map(|id| id.0).collect(),
        });
        Ok(node_id)
    }

    /// Register an **already-persisted** node's identity so a subsequent
    /// [`create_edge`](Self::create_edge) can resolve it as an endpoint — without
    /// writing a new node row or minting a fresh surrogate.
    ///
    /// Used by mixed `MATCH … CREATE …` execution (#703): a node bound by the
    /// preceding `MATCH` is referenced (its `node_uuid`/`node_id` come from the
    /// matched row), not created. Unlike [`create_node`](Self::create_node), this
    /// does **not** push a [`NodeRow`] or advance `next_node_id`; it only teaches
    /// the UUID→surrogate map.
    pub fn register_existing_node(&mut self, node_uuid: Uuid, node_id: u64) {
        self.uuid_to_node_id.insert(to_bytes(&node_uuid), node_id);
    }

    /// Return the surrogate ID for a node already known to this write session.
    /// This includes both nodes buffered earlier in the statement and persisted
    /// nodes registered from a matched input row.
    #[must_use]
    pub fn node_id_for_uuid(&self, node_uuid: &Uuid) -> Option<u64> {
        self.uuid_to_node_id.get(&to_bytes(node_uuid)).copied()
    }

    /// Buffer a new edge and return its assigned `edge_id` surrogate.
    ///
    /// Both endpoints must have been registered via
    /// [`create_node`](Self::create_node) first so their `node_id` surrogates
    /// can be resolved.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if either endpoint UUID is unknown.
    pub fn create_edge(
        &mut self,
        edge_uuid: Uuid,
        rel_type: &str,
        src_uuid: &Uuid,
        dst_uuid: &Uuid,
    ) -> Result<u64, GfError> {
        let src_bytes = to_bytes(src_uuid);
        let dst_bytes = to_bytes(dst_uuid);
        let src_id = *self.uuid_to_node_id.get(&src_bytes).ok_or_else(|| {
            GfError::Storage(format!(
                "create_edge: source {} has no node_id; call create_node first",
                graphforge_core::uuid::to_string(src_uuid)
            ))
        })?;
        let dst_id = *self.uuid_to_node_id.get(&dst_bytes).ok_or_else(|| {
            GfError::Storage(format!(
                "create_edge: destination {} has no node_id; call create_node first",
                graphforge_core::uuid::to_string(dst_uuid)
            ))
        })?;

        let edge_id = self.next_edge_id;
        self.next_edge_id += 1;

        let (stem, rel_type_name) = match self.mode {
            OntologyMode::Exploratory => (EXPLORATORY_STEM.to_owned(), Some(rel_type.to_owned())),
            OntologyMode::Advisory | OntologyMode::Strict => (rel_type.to_owned(), None),
        };

        self.edges.entry(stem).or_default().push(EdgeRow {
            edge_uuid: to_bytes(&edge_uuid),
            src_uuid: src_bytes,
            dst_uuid: dst_bytes,
            edge_id,
            src_id,
            dst_id,
            rel_type_name,
        });
        Ok(edge_id)
    }

    /// Buffer a property row for a node.
    ///
    /// In Strict / Advisory mode with a known `entity_type`, properties route to
    /// `properties/TYPENAME.parquet`; otherwise (exploratory, or no entity type)
    /// they route to `properties/_untyped.parquet`.
    ///
    /// # Errors
    /// Currently infallible; returns `Result` for forward compatibility.
    pub fn set_properties(
        &mut self,
        node_uuid: &Uuid,
        entity_type: Option<&str>,
        props: HashMap<String, IrLiteral>,
    ) -> Result<(), GfError> {
        let stem = match (self.mode, entity_type) {
            (OntologyMode::Advisory | OntologyMode::Strict, Some(t)) => t.to_owned(),
            _ => UNTYPED_STEM.to_owned(),
        };
        self.properties.entry(stem).or_default().push(PropRow {
            node_uuid: to_bytes(node_uuid),
            props,
        });
        Ok(())
    }

    /// Buffer a property row for an edge, keyed by `edge_uuid`.
    ///
    /// Edge properties route to `edge_properties/<REL_TYPE>.parquet` by relation
    /// name in **every** mode (unlike node properties, which fall back to
    /// `_untyped` in exploratory mode). The read side resolves the file stem from
    /// the relation name, so a single namespace keyed by rel type keeps write and
    /// read in lock-step and avoids colliding with the node `properties/`
    /// directory. A `None` `rel_type` (an edge created without a known relation
    /// name) routes to the `_untyped` catch-all.
    ///
    /// # Errors
    /// Currently infallible; returns `Result` for forward compatibility.
    pub fn set_edge_properties(
        &mut self,
        edge_uuid: &Uuid,
        rel_type: Option<&str>,
        props: HashMap<String, IrLiteral>,
    ) -> Result<(), GfError> {
        let stem = rel_type.unwrap_or(UNTYPED_STEM).to_owned();
        self.edge_properties
            .entry(stem)
            .or_default()
            .push(EdgePropRow {
                edge_uuid: to_bytes(edge_uuid),
                props,
            });
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Pending-buffer inspection and mutation (#792)
    //
    // A mixed write statement (CREATE … DELETE/SET/REMOVE …) needs later
    // clauses to see and edit the entities earlier clauses buffered: DELETE
    // must find edges created in-statement (and cancel them in the buffer
    // instead of rewriting files), and SET/REMOVE on a created entity must
    // land in its buffered rows (a file rewrite keyed on an uncommitted uuid
    // would miss).
    // -----------------------------------------------------------------------

    /// Whether a node with this uuid is buffered (created in this statement
    /// and not yet flushed or cancelled).
    #[must_use]
    pub fn contains_pending_node(&self, node_uuid: &[u8; 16]) -> bool {
        self.nodes.iter().any(|r| &r.node_uuid == node_uuid)
    }

    /// Return distinct label tokens on buffered nodes selected by UUID.
    #[must_use]
    pub fn pending_node_labels(&self, targets: &HashSet<[u8; 16]>) -> HashSet<u32> {
        self.nodes
            .iter()
            .filter(|row| targets.contains(&row.node_uuid))
            .flat_map(|row| row.type_ids.iter().copied())
            .collect()
    }

    /// Materialize the currently buffered node topology without consuming it.
    /// Statement-local reads use this as an in-memory overlay before commit.
    ///
    /// # Errors
    /// Returns [`GfError::Parquet`] if the buffered values cannot form the
    /// canonical topology batch.
    pub fn pending_nodes_batch(&self) -> Result<RecordBatch, GfError> {
        let n = self.nodes.len();
        if n == 0 {
            return Ok(RecordBatch::new_empty(TOPOLOGY_NODES_SCHEMA.clone()));
        }
        let uuids =
            FixedSizeBinaryArray::try_from_iter(self.nodes.iter().map(|r| r.node_uuid.to_vec()))
                .map_err(pq_err)?;
        let node_ids = UInt64Array::from(self.nodes.iter().map(|r| r.node_id).collect::<Vec<_>>());
        let type_ids = UInt32Array::from(self.nodes.iter().map(|r| r.type_id).collect::<Vec<_>>());
        let nullable_label_sets =
            arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
                self.nodes
                    .iter()
                    .map(|row| Some(row.type_ids.iter().copied().map(Some))),
            );
        let label_sets = arrow::array::ListArray::new(
            Arc::new(Field::new("item", DataType::UInt32, false)),
            nullable_label_sets.offsets().clone(),
            nullable_label_sets.values().clone(),
            None,
        );
        let ts = self.timestamp_array(n);
        RecordBatch::try_new(
            TOPOLOGY_NODES_SCHEMA.clone(),
            vec![
                Arc::new(uuids),
                Arc::new(node_ids),
                Arc::new(type_ids),
                Arc::new(label_sets),
                Arc::new(ts.clone()),
                Arc::new(ts),
            ],
        )
        .map_err(pq_err)
    }

    /// Find a buffered node whose labels and properties satisfy a MERGE pattern.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn find_pending_node(
        &self,
        labels: &[u32],
        properties: &[(String, IrLiteral)],
    ) -> Option<PendingNodeMatch> {
        self.find_pending_nodes(labels, properties)
            .into_iter()
            .next()
    }

    /// Return every buffered node matching all requested labels and properties.
    #[must_use]
    pub fn find_pending_nodes(
        &self,
        labels: &[u32],
        properties: &[(String, IrLiteral)],
    ) -> Vec<PendingNodeMatch> {
        self.nodes
            .iter()
            .filter_map(|node| {
                if !labels.iter().all(|wanted| node.type_ids.contains(wanted)) {
                    return None;
                }
                let props = self
                    .properties
                    .values()
                    .flatten()
                    .filter(|row| row.node_uuid == node.node_uuid)
                    .flat_map(|row| {
                        row.props
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                properties
                    .iter()
                    .all(|(name, value)| props.get(name) == Some(value))
                    .then(|| {
                        (
                            node.node_uuid,
                            node.node_id,
                            node.type_id,
                            node.type_ids.clone(),
                            props,
                        )
                    })
            })
            .collect()
    }

    /// Whether an edge with this uuid is buffered.
    #[must_use]
    pub fn contains_pending_edge(&self, edge_uuid: &[u8; 16]) -> bool {
        self.edges
            .values()
            .any(|rows| rows.iter().any(|r| &r.edge_uuid == edge_uuid))
    }

    /// Find a buffered edge matching type, endpoints, direction, and properties.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn find_pending_edge(
        &self,
        rel_type: &str,
        src: &[u8; 16],
        dst: &[u8; 16],
        undirected: bool,
        properties: &[(String, IrLiteral)],
    ) -> Option<([u8; 16], [u8; 16], [u8; 16], HashMap<String, IrLiteral>)> {
        self.edges.iter().find_map(|(stem, edges)| {
            edges.iter().find_map(|edge| {
                let edge_type = edge.rel_type_name.as_deref().unwrap_or(stem);
                let direct = edge.src_uuid == *src && edge.dst_uuid == *dst;
                let reverse = edge.src_uuid == *dst && edge.dst_uuid == *src;
                if edge_type != rel_type || !(direct || undirected && reverse) {
                    return None;
                }
                let props = self
                    .edge_properties
                    .values()
                    .flatten()
                    .filter(|row| row.edge_uuid == edge.edge_uuid)
                    .flat_map(|row| {
                        row.props
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                properties
                    .iter()
                    .all(|(name, value)| props.get(name) == Some(value))
                    .then_some((edge.edge_uuid, edge.src_uuid, edge.dst_uuid, props))
            })
        })
    }

    /// The uuids of buffered edges incident (as src or dst) to any of `nodes`.
    ///
    /// The pending complement of
    /// [`incident_edge_uuids`](crate::incident_edge_uuids), which only sees
    /// committed files: openCypher's "cannot delete a node that still has
    /// relationships" must also count edges created earlier in the same
    /// statement.
    #[must_use]
    pub fn pending_incident_edge_uuids<S: std::hash::BuildHasher>(
        &self,
        nodes: &HashSet<[u8; 16], S>,
    ) -> Vec<[u8; 16]> {
        self.edges
            .values()
            .flatten()
            .filter(|r| nodes.contains(&r.src_uuid) || nodes.contains(&r.dst_uuid))
            .map(|r| r.edge_uuid)
            .collect()
    }

    /// Drop buffered nodes (and their buffered property rows) whose uuid is in
    /// `targets`, so a created-then-deleted node never hits disk. Forgets the
    /// uuid→surrogate mapping too: the entity no longer exists, so a later
    /// `create_edge` referencing it must fail. Returns the node rows dropped.
    pub fn cancel_nodes<S: std::hash::BuildHasher>(
        &mut self,
        targets: &HashSet<[u8; 16], S>,
    ) -> u64 {
        let before = self.nodes.len();
        self.nodes.retain(|r| !targets.contains(&r.node_uuid));
        let dropped = (before - self.nodes.len()) as u64;
        // Drop emptied stems too — flush builds columns per buffered stem and
        // a zero-row stem has nothing to build.
        self.properties.retain(|_, rows| {
            rows.retain(|r| !targets.contains(&r.node_uuid));
            !rows.is_empty()
        });
        self.uuid_to_node_id
            .retain(|uuid, _| !targets.contains(uuid));
        dropped
    }

    /// Drop buffered edges (and their buffered property rows) whose uuid is in
    /// `targets`. Returns the edge rows dropped.
    pub fn cancel_edges<S: std::hash::BuildHasher>(
        &mut self,
        targets: &HashSet<[u8; 16], S>,
    ) -> u64 {
        let mut dropped = 0u64;
        self.edges.retain(|_, rows| {
            let before = rows.len();
            rows.retain(|r| !targets.contains(&r.edge_uuid));
            dropped += (before - rows.len()) as u64;
            !rows.is_empty()
        });
        self.edge_properties.retain(|_, rows| {
            rows.retain(|r| !targets.contains(&r.edge_uuid));
            !rows.is_empty()
        });
        dropped
    }

    /// Merge `props` into the buffered property row of a pending node
    /// (SET on an entity created earlier in this statement), inserting a row
    /// if it has none yet. Same stem routing as
    /// [`set_properties`](Self::set_properties).
    pub fn merge_pending_node_props(
        &mut self,
        node_uuid: &[u8; 16],
        entity_type: Option<&str>,
        props: HashMap<String, IrLiteral>,
    ) {
        let stem = match (self.mode, entity_type) {
            (OntologyMode::Advisory | OntologyMode::Strict, Some(t)) => t.to_owned(),
            _ => UNTYPED_STEM.to_owned(),
        };
        let rows = self.properties.entry(stem).or_default();
        if let Some(row) = rows.iter_mut().find(|r| &r.node_uuid == node_uuid) {
            row.props.extend(props);
        } else {
            rows.push(PropRow {
                node_uuid: *node_uuid,
                props,
            });
        }
    }

    /// Add labels to a node buffered by this writer, preserving its primary label.
    pub fn add_pending_node_labels(&mut self, node_uuid: &[u8; 16], labels: &[u32]) -> u64 {
        let Some(row) = self
            .nodes
            .iter_mut()
            .find(|row| &row.node_uuid == node_uuid)
        else {
            return 0;
        };
        let before = row.type_ids.len();
        row.type_ids.extend(labels.iter().copied());
        row.type_ids.sort_unstable();
        row.type_ids.dedup();
        (row.type_ids.len() - before) as u64
    }

    /// Remove labels from a node buffered by this writer. The immutable scalar
    /// `type_id` remains only as the property-file routing key; `type_ids` is
    /// the authoritative membership set.
    pub fn remove_pending_node_labels(&mut self, node_uuid: &[u8; 16], labels: &[u32]) -> u64 {
        let Some(row) = self
            .nodes
            .iter_mut()
            .find(|row| &row.node_uuid == node_uuid)
        else {
            return 0;
        };
        let before = row.type_ids.len();
        row.type_ids.retain(|label| !labels.contains(label));
        (before - row.type_ids.len()) as u64
    }

    /// Edge analogue of
    /// [`merge_pending_node_props`](Self::merge_pending_node_props); same stem
    /// routing as [`set_edge_properties`](Self::set_edge_properties).
    pub fn merge_pending_edge_props(
        &mut self,
        edge_uuid: &[u8; 16],
        rel_type: Option<&str>,
        props: HashMap<String, IrLiteral>,
    ) {
        let stem = rel_type.unwrap_or(UNTYPED_STEM).to_owned();
        let rows = self.edge_properties.entry(stem).or_default();
        if let Some(row) = rows.iter_mut().find(|r| &r.edge_uuid == edge_uuid) {
            row.props.extend(props);
        } else {
            rows.push(EdgePropRow {
                edge_uuid: *edge_uuid,
                props,
            });
        }
    }

    /// Remove `keys` from a pending node's buffered property rows (REMOVE on
    /// an entity created earlier in this statement). Absent keys/rows are
    /// no-ops (openCypher). Scans every stem — a REMOVE clause does not know
    /// the routing the CREATE used.
    pub fn remove_pending_node_props(&mut self, node_uuid: &[u8; 16], keys: &HashSet<String>) {
        for rows in self.properties.values_mut() {
            for row in rows.iter_mut().filter(|r| &r.node_uuid == node_uuid) {
                row.props.retain(|k, _| !keys.contains(k));
            }
        }
    }

    /// Edge analogue of
    /// [`remove_pending_node_props`](Self::remove_pending_node_props).
    pub fn remove_pending_edge_props(&mut self, edge_uuid: &[u8; 16], keys: &HashSet<String>) {
        for rows in self.edge_properties.values_mut() {
            for row in rows.iter_mut().filter(|r| &r.edge_uuid == edge_uuid) {
                row.props.retain(|k, _| !keys.contains(k));
            }
        }
    }

    /// Merge all buffered rows with any existing on-disk data and write the
    /// result, then clear the row buffers.
    ///
    /// Only creates a subdirectory when there are rows to write into it.  Each
    /// target file is read, concatenated with the new rows (property files are
    /// decoded and re-inferred so the dynamic schema evolves), and rewritten —
    /// so separate write sessions accumulate (#733).  All files stage and
    /// commit as one batch (#790), nodes first: a failure while building any
    /// file leaves the prior state fully intact, and a (rare) rename-phase
    /// failure can commit a node without its edges, never the reverse.
    ///
    /// A batch that stages topology files bumps the project
    /// `topology_generation` counter before committing (#759); property-only
    /// flushes do not bump.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
    pub fn flush(&mut self) -> Result<(), GfError> {
        let mut staged = RewriteBatch::new();
        self.flush_into(&mut staged)?;
        let pending = self.take_pending_delta();
        if let Some(generation) = crate::generation::commit_topology_aware(staged, &self.dir)? {
            // A pure-append flush (only CREATEs reach `GraphWriter`): record the
            // delta segment so the adjacency index can serve the new edges
            // without a rebuild. A node-only flush writes an empty segment so
            // the chain stays contiguous. See `write_segment_best_effort`.
            self.write_segment_best_effort(generation, &pending);
        }
        Ok(())
    }

    /// Drain the edges captured for the next adjacency delta segment. The
    /// statement driver (#792) calls this after `flush_into` to write or
    /// discard the segment around its own commit (#765).
    #[must_use]
    pub fn take_pending_delta(&mut self) -> Vec<crate::adjacency_delta::DeltaEdge> {
        let mut edges = std::mem::take(&mut self.pending_delta);
        // Ascending edge_id = creation order (edges buffer per stem, so the
        // drain interleaves stems); the segment's documented order. Correctness
        // does not depend on it — `apply_delta_segments` re-sorts by (key, edge).
        edges.sort_unstable_by_key(|e| e.edge_id);
        edges
    }

    /// Best-effort write of the delta segment for `generation` — only when the
    /// adjacency capability directory exists (never grow `deltas/` for a project
    /// that has no index). A failed write costs at most one future rebuild and
    /// must never fail a already-committed flush, so the error is swallowed.
    pub fn write_segment_best_effort(
        &self,
        generation: u64,
        edges: &[crate::adjacency_delta::DeltaEdge],
    ) {
        if crate::adjacency::adjacency_dir(&self.dir).exists() {
            let _ = crate::adjacency_delta::write_delta_segment(&self.dir, generation, edges);
        }
    }

    /// Stage all buffered rows into `staged` (committed by the caller) and
    /// clear the row buffers.
    ///
    /// Reads **through** `staged` and restages: a file this statement already
    /// staged (e.g. a DELETE rewrite of the same property file, #792) is the
    /// merge base and its entry is replaced in place with the net content —
    /// files new to the batch append after it, so created edges commit after
    /// `topology/nodes.parquet` whether or not a delete staged it earlier.
    ///
    /// On success the buffers are cleared even though nothing is committed
    /// yet; the writer is not reusable if the caller's commit fails.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
    pub fn flush_into(&mut self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        self.flush_nodes(staged)?;
        self.flush_edges(staged)?;
        self.flush_properties(staged)?;
        self.flush_edge_properties(staged)?;
        Ok(())
    }

    fn flush_nodes(&mut self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        if self.nodes.is_empty() {
            return Ok(());
        }
        let topology = self.dir.join("topology");
        fs::create_dir_all(&topology).map_err(|e| io_err(&e))?;

        let batch = self.pending_nodes_batch()?;

        // Merge with any rows already on disk so separate write sessions
        // accumulate (#733) rather than overwriting. The schema is fixed, so a
        // concat of [existing, new] always succeeds.
        let path = topology.join("nodes.parquet");
        let existing_rows = staged.restage_append_with(
            &path,
            TOPOLOGY_NODES_SCHEMA.clone(),
            &batch,
            |existing| {
                crate::catalog::normalize_topology_nodes(vec![existing])
                    .map_err(pq_err)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| GfError::Storage("node normalization returned no batch".into()))
            },
        )?;
        crate::io_stats::record_topology_rewrite(existing_rows, batch.num_rows() as u64);
        self.nodes.clear();
        Ok(())
    }

    fn flush_edges(&mut self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        if self.edges.is_empty() {
            return Ok(());
        }
        let edges_dir = self.dir.join("topology").join("edges");
        fs::create_dir_all(&edges_dir).map_err(|e| io_err(&e))?;

        // Drain so we don't hold a borrow on self.edges while writing.
        let buffered: Vec<(String, Vec<EdgeRow>)> = self.edges.drain().collect();
        for (stem, rows) in buffered {
            let exploratory = stem == EXPLORATORY_STEM;
            // Capture created edges for the adjacency delta segment (#765): the
            // typed stem is the relation name; exploratory rows carry their own.
            for r in &rows {
                self.pending_delta.push(crate::adjacency_delta::DeltaEdge {
                    rel_type_name: if exploratory {
                        r.rel_type_name.clone().unwrap_or_default()
                    } else {
                        stem.clone()
                    },
                    edge_id: r.edge_id,
                    src_id: r.src_id,
                    dst_id: r.dst_id,
                });
            }
            let schema = if exploratory {
                EXPLORATORY_EDGE_SCHEMA.clone()
            } else {
                TYPED_EDGE_SCHEMA.clone()
            };
            let schema = self.authenticated_route_schema(schema, &stem);
            let batch = self.edge_batch(&rows, &schema, exploratory)?;
            // Merge with this stem's existing file so appends accumulate (#733);
            // stems not in this buffer are never opened, so they are untouched.
            let path = edges_dir.join(format!("{stem}.parquet"));
            let existing_rows = staged.restage_append(&path, schema, &batch)?;
            crate::io_stats::record_topology_rewrite(existing_rows, batch.num_rows() as u64);
        }
        Ok(())
    }

    fn edge_batch(
        &self,
        rows: &[EdgeRow],
        schema: &SchemaRef,
        exploratory: bool,
    ) -> Result<RecordBatch, GfError> {
        let edge_uuids =
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.edge_uuid.to_vec()))
                .map_err(pq_err)?;
        let src_uuids =
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.src_uuid.to_vec()))
                .map_err(pq_err)?;
        let dst_uuids =
            FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.dst_uuid.to_vec()))
                .map_err(pq_err)?;
        let edge_ids = UInt64Array::from(rows.iter().map(|r| r.edge_id).collect::<Vec<_>>());
        let src_ids = UInt64Array::from(rows.iter().map(|r| r.src_id).collect::<Vec<_>>());
        let dst_ids = UInt64Array::from(rows.iter().map(|r| r.dst_id).collect::<Vec<_>>());
        let ts = self.timestamp_array(rows.len());
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(edge_uuids),
            Arc::new(src_uuids),
            Arc::new(dst_uuids),
            Arc::new(edge_ids),
            Arc::new(src_ids),
            Arc::new(dst_ids),
            Arc::new(ts),
        ];
        if exploratory {
            let names = StringArray::from(
                rows.iter()
                    .map(|r| r.rel_type_name.clone().unwrap_or_default())
                    .collect::<Vec<_>>(),
            );
            cols.push(Arc::new(names));
        }
        RecordBatch::try_new(schema.clone(), cols).map_err(pq_err)
    }

    fn flush_properties(&mut self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        if self.properties.is_empty() {
            return Ok(());
        }
        let buffered: Vec<(String, Vec<PropRow>)> = self.properties.drain().collect();
        for (stem, new_rows) in buffered {
            let rows = merge_node_property_window(new_rows);
            let (schema, cols) = build_property_columns(&stem, &rows)?;
            let schema = self.authenticated_route_schema(Arc::new(schema), &stem);
            stage_property_fragment(
                staged,
                &self.dir,
                crate::property_overlay::PropertyRouteKind::Node,
                &stem,
                schema,
                cols,
                false,
            )?;
        }
        Ok(())
    }

    /// Edge analogue of [`flush_properties`](Self::flush_properties): merge the
    /// buffered edge-property rows with any on-disk rows (decode + re-infer) and
    /// stage `edge_properties/<stem>.parquet`. The join key is `edge_uuid`.
    fn flush_edge_properties(&mut self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        if self.edge_properties.is_empty() {
            return Ok(());
        }
        let buffered: Vec<(String, Vec<EdgePropRow>)> = self.edge_properties.drain().collect();
        for (stem, new_rows) in buffered {
            let rows = merge_edge_property_window(new_rows);
            let (schema, cols) = build_property_columns_keyed(
                EDGE_PROPERTY_UUID_FIELD,
                "graphforge.rel_type",
                &stem,
                &rows,
            )?;
            let schema = self.authenticated_route_schema(Arc::new(schema), &stem);
            stage_property_fragment(
                staged,
                &self.dir,
                crate::property_overlay::PropertyRouteKind::Edge,
                &stem,
                schema,
                cols,
                false,
            )?;
        }
        Ok(())
    }

    fn timestamp_array(&self, n: usize) -> TimestampMicrosecondArray {
        TimestampMicrosecondArray::from(vec![self.now_micros; n])
            .with_timezone_opt(Some(Arc::from("UTC")))
    }

    fn authenticated_route_schema(&self, schema: SchemaRef, stem: &str) -> SchemaRef {
        match (
            &self.semantic_composition_fingerprint,
            stem.starts_with("s-"),
        ) {
            (Some(fingerprint), true) => Arc::new(crate::schemas::with_semantic_route_metadata(
                schema.as_ref(),
                stem,
                fingerprint,
            )),
            _ => schema,
        }
    }
}

fn preserve_semantic_route_metadata(
    schema: SchemaRef,
    existing: Option<&HashMap<String, String>>,
) -> SchemaRef {
    let Some(existing) = existing else {
        return schema;
    };
    let mut metadata = schema.metadata().clone();
    for key in [
        crate::SEMANTIC_ROUTE_METADATA_KEY,
        crate::SEMANTIC_COMPOSITION_METADATA_KEY,
    ] {
        if !metadata.contains_key(key)
            && let Some(value) = existing.get(key)
        {
            metadata.insert(key.to_owned(), value.clone());
        }
    }
    Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata))
}

// ---------------------------------------------------------------------------
// Property schema inference
// ---------------------------------------------------------------------------

/// Arrow type a property column is built as, inferred from the literals seen.
// Not `Copy`: `List` boxes its inner type (a homogeneous `List<inner>` column).
#[derive(Clone, PartialEq, Eq)]
enum ColType {
    Int,
    Float,
    Bool,
    Str,
    HetScalar,
    Duration,
    DateTime,
    Date,
    LocalDateTime,
    Time,
    ZonedTime,
    ZonedDateTime,
    Spatial(SpatialType, Option<String>, Option<String>),
    /// A homogeneous `List<inner>` column (#1006).
    List(Box<ColType>),
}

impl ColType {
    fn of(lit: &IrLiteral) -> Option<Self> {
        match lit {
            IrLiteral::Null
            // Query-parameter maps are not a storage property type.
            | IrLiteral::Map(_)
            // Typed UUID parameters are identity predicates, not properties.
            | IrLiteral::Uuid(_) => None,
            IrLiteral::Int(_) => Some(Self::Int),
            IrLiteral::Float(_) => Some(Self::Float),
            IrLiteral::Bool(_) => Some(Self::Bool),
            IrLiteral::Str(_) => Some(Self::Str),
            IrLiteral::Duration { .. } => Some(Self::Duration),
            IrLiteral::DateTime(_) => Some(Self::DateTime),
            IrLiteral::Date(_) => Some(Self::Date),
            IrLiteral::LocalDateTime { .. } => Some(Self::LocalDateTime),
            IrLiteral::Time(_) => Some(Self::Time),
            IrLiteral::ZonedTime { .. } => Some(Self::ZonedTime),
            IrLiteral::ZonedDateTime { .. } => Some(Self::ZonedDateTime),
            IrLiteral::Spatial(value) => Some(Self::Spatial(
                value.spatial_type.clone(),
                value.extension_name.clone(),
                value.extension_metadata.clone(),
            )),
            // A homogeneous list: infer the inner type from the first non-null
            // element. A list whose elements are all null (or an empty list)
            // yields no type and the column falls back to `Str`. (#1006)
            IrLiteral::List(items) => items
                .iter()
                .find_map(Self::of)
                .map(|inner| Self::List(Box::new(inner))),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            // `Str` is also the coercion target for mixed-type columns.
            Self::Str => DataType::Utf8,
            Self::HetScalar => DataType::Struct(heterogeneous_scalar_fields()),
            Self::Duration => DataType::Struct(crate::schemas::duration_struct_fields()),
            Self::DateTime => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Date => DataType::Struct(crate::schemas::date_struct_fields()),
            Self::LocalDateTime => DataType::Struct(crate::schemas::localdatetime_struct_fields()),
            Self::Time => DataType::Time64(TimeUnit::Nanosecond),
            Self::ZonedTime => DataType::Struct(crate::schemas::time_struct_fields()),
            Self::ZonedDateTime => DataType::Struct(crate::schemas::datetime_struct_fields()),
            Self::Spatial(spatial_type, _, _) => spatial_data_type(spatial_type),
            Self::List(inner) => {
                DataType::List(Arc::new(Field::new("item", inner.data_type(), true)))
            }
        }
    }

    fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::Int | Self::Float | Self::Bool | Self::Str | Self::HetScalar
        )
    }
}

fn spatial_data_type(spatial_type: &SpatialType) -> DataType {
    let coordinate = DataType::Struct(arrow::datatypes::Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]));
    let list = |name: &str, child| DataType::List(Arc::new(Field::new(name, child, false)));
    match spatial_type.geometry {
        SpatialGeometryType::Point => coordinate,
        SpatialGeometryType::LineString => list("vertices", coordinate),
        SpatialGeometryType::Polygon => list("rings", list("vertices", coordinate)),
        SpatialGeometryType::MultiPoint => list("points", coordinate),
        SpatialGeometryType::MultiLineString => list("linestrings", list("vertices", coordinate)),
        SpatialGeometryType::MultiPolygon => {
            list("polygons", list("rings", list("vertices", coordinate)))
        }
    }
}

fn spatial_field(
    name: &str,
    spatial_type: &SpatialType,
    preserved_name: Option<&str>,
    preserved_metadata: Option<&str>,
    nullable: bool,
) -> Field {
    let canonical_name = match spatial_type.geometry {
        SpatialGeometryType::Point => "geoarrow.point",
        SpatialGeometryType::LineString => "geoarrow.linestring",
        SpatialGeometryType::Polygon => "geoarrow.polygon",
        SpatialGeometryType::MultiPoint => "geoarrow.multipoint",
        SpatialGeometryType::MultiLineString => "geoarrow.multilinestring",
        SpatialGeometryType::MultiPolygon => "geoarrow.multipolygon",
    };
    let crs = match &spatial_type.crs {
        SpatialCrs::Epsg4326 => "EPSG:4326",
        SpatialCrs::Epsg3857 => "EPSG:3857",
        SpatialCrs::Preserved(value) => value,
    };
    let extension_name = preserved_name.unwrap_or(canonical_name);
    let extension_metadata = preserved_metadata.map_or_else(
        || format!("{{\"crs\":\"{crs}\",\"crs_type\":\"authority_code\"}}"),
        ToOwned::to_owned,
    );
    Field::new(name, spatial_data_type(spatial_type), nullable).with_metadata(HashMap::from([
        ("ARROW:extension:name".to_owned(), extension_name.to_owned()),
        ("ARROW:extension:metadata".to_owned(), extension_metadata),
    ]))
}

fn heterogeneous_scalar_fields() -> arrow::datatypes::Fields {
    arrow::datatypes::Fields::from(vec![
        Field::new("__het_tag", DataType::Int8, false),
        Field::new("__het_int", DataType::Int64, true),
        Field::new("__het_float", DataType::Float64, true),
        Field::new("__het_str", DataType::Utf8, true),
        Field::new("__het_bool", DataType::Boolean, true),
    ])
}

/// Build the dynamic schema and column arrays for a **node**-property file
/// (join key `node_uuid`, metadata key `graphforge.entity_type`).
fn build_property_columns(
    entity_type: &str,
    rows: &[PropRow],
) -> Result<(Schema, Vec<ArrayRef>), GfError> {
    build_property_columns_keyed(
        NODE_PROPERTY_UUID_FIELD,
        "graphforge.entity_type",
        entity_type,
        rows,
    )
}

/// Build the dynamic schema and column arrays for a property file, keyed by an
/// arbitrary uuid join column.
///
/// Shared by node properties (`node_uuid`) and edge properties (`edge_uuid`).
/// Column order is the first-seen order of property names across `rows`
/// (deterministic).  Each column's type is inferred from the first non-null
/// value; conflicting scalar types use a tagged struct that preserves each value.
///
/// `uuid_field_name` is the leading join-key column; `meta_key`/`meta_value`
/// is the schema-level metadata identifying the file's entity or relation type.
fn build_property_columns_keyed<R: PropRowLike>(
    uuid_field_name: &str,
    meta_key: &str,
    meta_value: &str,
    rows: &[R],
) -> Result<(Schema, Vec<ArrayRef>), GfError> {
    // First-seen-ordered list of property names + inferred column type.
    // `seen` tracks column order independently of `col_types`: a column may be
    // seen (ordered) before any concrete value fixes its type, so order-dedup
    // must not key on `col_types` membership.
    let mut order: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut col_types: HashMap<String, ColType> = HashMap::new();
    for row in rows {
        for (name, lit) in row.props() {
            reject_map_property_value(name, lit)?;
            if seen.insert(name.clone()) {
                order.push(name.clone());
            }
            // Only concrete literals contribute a type; `Null` contributes
            // none, so the first *non-null* value determines the column type.
            // A column that never sees a concrete value defaults to `Str` via
            // `unwrap_or(ColType::Str)` below.
            if let Some(t) = ColType::of(lit) {
                col_types
                    .entry(name.clone())
                    .and_modify(|existing| {
                        if *existing != t {
                            *existing = if existing.is_scalar() && t.is_scalar() {
                                ColType::HetScalar
                            } else {
                                ColType::Str
                            };
                        }
                    })
                    .or_insert(t);
            }
        }
    }

    let mut fields: Vec<Field> = vec![uuid_field(uuid_field_name)];
    for name in &order {
        let ct = col_types.get(name).cloned().unwrap_or(ColType::Str);
        fields.push(match ct {
            ColType::Spatial(spatial_type, extension_name, extension_metadata) => spatial_field(
                name,
                &spatial_type,
                extension_name.as_deref(),
                extension_metadata.as_deref(),
                true,
            ),
            _ => Field::new(name, ct.data_type(), true),
        });
    }
    let meta: HashMap<String, String> = [(meta_key.to_owned(), meta_value.to_owned())]
        .into_iter()
        .collect();
    let schema = Schema::new(fields).with_metadata(meta);

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(order.len() + 1);
    let uuids = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.uuid_bytes().to_vec()))
        .map_err(pq_err)?;
    cols.push(Arc::new(uuids));

    for name in &order {
        let ct = col_types.get(name).cloned().unwrap_or(ColType::Str);
        cols.push(build_property_array(name, ct, rows));
    }
    Ok((schema, cols))
}

pub(crate) fn property_snapshots_to_batch(
    route: &str,
    is_edge: bool,
    rows: Vec<crate::property_overlay::PropertySnapshotRow>,
) -> Result<Option<RecordBatch>, GfError> {
    if rows.is_empty() {
        return Ok(None);
    }
    if is_edge {
        let rows = rows
            .into_iter()
            .map(|row| EdgePropRow {
                edge_uuid: row.uuid,
                props: row.values.into_iter().collect(),
            })
            .collect::<Vec<_>>();
        let (schema, columns) = build_property_columns_keyed(
            EDGE_PROPERTY_UUID_FIELD,
            "graphforge.rel_type",
            route,
            &rows,
        )?;
        RecordBatch::try_new(Arc::new(schema), columns)
            .map(Some)
            .map_err(pq_err)
    } else {
        let rows = rows
            .into_iter()
            .map(|row| PropRow {
                node_uuid: row.uuid,
                props: row.values.into_iter().collect(),
            })
            .collect::<Vec<_>>();
        let (schema, columns) = build_property_columns(route, &rows)?;
        RecordBatch::try_new(Arc::new(schema), columns)
            .map(Some)
            .map_err(pq_err)
    }
}

fn reject_map_property_value(name: &str, lit: &IrLiteral) -> Result<(), GfError> {
    if contains_uuid_literal(lit) {
        return Err(GfError::Validation(format!(
            "property `{name}` cannot store typed UUID query parameters"
        )));
    }
    if contains_map_literal(lit) {
        return Err(GfError::Storage(format!(
            "property `{name}` cannot store map values"
        )));
    }
    Ok(())
}

fn contains_map_literal(lit: &IrLiteral) -> bool {
    match lit {
        IrLiteral::Map(_) => true,
        IrLiteral::List(items) => items.iter().any(contains_map_literal),
        _ => false,
    }
}

fn contains_uuid_literal(lit: &IrLiteral) -> bool {
    match lit {
        IrLiteral::Uuid(_) => true,
        IrLiteral::List(items) => items.iter().any(contains_uuid_literal),
        IrLiteral::Map(entries) => entries
            .iter()
            .any(|(_, value)| contains_uuid_literal(value)),
        _ => false,
    }
}

/// Build one nullable property column, appending nulls for rows that omit the
/// property (or whose value does not match the column's inferred type — those
/// are stringified when the column is `Str`, else null).
#[allow(
    clippy::too_many_lines,
    reason = "one builder arm per ColType; the per-type append loops read clearest inline"
)]
fn build_property_array<R: PropRowLike>(name: &str, ct: ColType, rows: &[R]) -> ArrayRef {
    match ct {
        ColType::Int => {
            let mut b = Int64Builder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Int(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::Float => {
            let mut b = Float64Builder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Float(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::Bool => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Bool(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::Duration => {
            // A typed duration is a `Struct{months, days, seconds, nanos}` (all
            // Int64; Parquet cannot persist Arrow `Interval`); shared field defs
            // via `duration_struct_fields`.
            use arrow::array::{Int64Builder, StructArray};
            use arrow::buffer::NullBuffer;
            let (mut mb, mut db, mut sb, mut nb) = (
                Int64Builder::new(),
                Int64Builder::new(),
                Int64Builder::new(),
                Int64Builder::new(),
            );
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::Duration {
                    months,
                    days,
                    seconds,
                    nanos,
                }) = row.props().get(name)
                {
                    mb.append_value(*months);
                    db.append_value(*days);
                    sb.append_value(*seconds);
                    nb.append_value(*nanos);
                    valid.push(true);
                } else {
                    mb.append_null();
                    db.append_null();
                    sb.append_null();
                    nb.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::duration_struct_fields(),
                vec![
                    Arc::new(mb.finish()),
                    Arc::new(db.finish()),
                    Arc::new(sb.finish()),
                    Arc::new(nb.finish()),
                ],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::DateTime => {
            let mut b = TimestampMicrosecondBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::DateTime(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish().with_timezone_opt(Some(Arc::from("UTC"))))
        }
        ColType::Date => {
            // A typed `date` is a self-describing `Struct{epoch_day: Int64}` —
            // i64 days, full year range (#1011).
            use arrow::array::{Int64Builder, StructArray};
            use arrow::buffer::NullBuffer;
            let mut b = Int64Builder::new();
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::Date(v)) = row.props().get(name) {
                    b.append_value(*v);
                    valid.push(true);
                } else {
                    b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::date_struct_fields(),
                vec![Arc::new(b.finish())],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::LocalDateTime => {
            // A typed `localdatetime` is a `Struct{date: Int64, time: Time64(ns)}`
            // (shared field defs via `localdatetime_struct_fields`).
            use arrow::array::{Int64Builder, StructArray, Time64NanosecondBuilder};
            use arrow::buffer::NullBuffer;
            let (mut date_b, mut time_b) = (Int64Builder::new(), Time64NanosecondBuilder::new());
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::LocalDateTime { days, nanos }) = row.props().get(name) {
                    date_b.append_value(*days);
                    time_b.append_value(*nanos);
                    valid.push(true);
                } else {
                    date_b.append_null();
                    time_b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::localdatetime_struct_fields(),
                vec![Arc::new(date_b.finish()), Arc::new(time_b.finish())],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::Time => {
            use arrow::array::Time64NanosecondBuilder;
            let mut b = Time64NanosecondBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Time(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::ZonedTime => {
            // A typed `time` is a `Struct{time: Time64(ns), offset: Int32}`.
            use arrow::array::{Int32Builder, StructArray, Time64NanosecondBuilder};
            use arrow::buffer::NullBuffer;
            let (mut time_b, mut off_b) = (Time64NanosecondBuilder::new(), Int32Builder::new());
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::ZonedTime { nanos, offset }) = row.props().get(name) {
                    time_b.append_value(*nanos);
                    off_b.append_value(*offset);
                    valid.push(true);
                } else {
                    time_b.append_null();
                    off_b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::time_struct_fields(),
                vec![Arc::new(time_b.finish()), Arc::new(off_b.finish())],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::ZonedDateTime => {
            // A typed `datetime` is a
            // `Struct{date: Int64, time: Time64(ns), offset: Int32, zone: Utf8}`.
            use arrow::array::{
                Int32Builder, Int64Builder, StringBuilder, StructArray, Time64NanosecondBuilder,
            };
            use arrow::buffer::NullBuffer;
            let (mut date_b, mut time_b, mut off_b, mut zone_b) = (
                Int64Builder::new(),
                Time64NanosecondBuilder::new(),
                Int32Builder::new(),
                StringBuilder::new(),
            );
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::ZonedDateTime {
                    days,
                    nanos,
                    offset,
                    zone,
                }) = row.props().get(name)
                {
                    date_b.append_value(*days);
                    time_b.append_value(*nanos);
                    off_b.append_value(*offset);
                    // None (offset-only) is stored as a NULL zone, not "".
                    zone_b.append_option(zone.as_deref());
                    valid.push(true);
                } else {
                    date_b.append_null();
                    time_b.append_null();
                    off_b.append_null();
                    zone_b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::datetime_struct_fields(),
                vec![
                    Arc::new(date_b.finish()),
                    Arc::new(time_b.finish()),
                    Arc::new(off_b.finish()),
                    Arc::new(zone_b.finish()),
                ],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::Spatial(spatial_type, _, _) => build_spatial_array(name, &spatial_type, rows),
        ColType::Str => {
            let mut b = StringBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Null) | None => b.append_null(),
                    Some(other) => b.append_value(literal_to_string(other)),
                }
            }
            Arc::new(b.finish())
        }
        ColType::HetScalar => build_heterogeneous_scalar_array(name, rows),
        ColType::List(inner) => {
            // Flatten every list's elements into one-property synthetic rows, then
            // build the child array with the SAME per-type machinery (so temporal
            // element types reuse their struct builders); `offsets` delimit each
            // row's slice, and a row that is not a list is a null list slot. (#1006)
            use arrow::array::ListArray;
            use arrow::buffer::{NullBuffer, OffsetBuffer};
            let mut elem_rows: Vec<PropRow> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::List(items)) = row.props().get(name) {
                    for it in items {
                        let mut props = HashMap::with_capacity(1);
                        props.insert("item".to_string(), it.clone());
                        elem_rows.push(PropRow {
                            node_uuid: [0u8; 16],
                            props,
                        });
                    }
                    valid.push(true);
                } else {
                    valid.push(false);
                }
                offsets.push(i32::try_from(elem_rows.len()).unwrap_or(i32::MAX));
            }
            let child = build_property_array("item", (*inner).clone(), &elem_rows);
            let field = Arc::new(Field::new("item", inner.data_type(), true));
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(offsets.into()),
                child,
                Some(NullBuffer::from(valid)),
            ))
        }
    }
}

fn coordinate_builder(capacity: usize) -> arrow::array::StructBuilder {
    use arrow::array::{ArrayBuilder, Float64Builder, StructBuilder};
    let DataType::Struct(fields) = spatial_data_type(&SpatialType {
        geometry: SpatialGeometryType::Point,
        crs: SpatialCrs::Epsg4326,
    }) else {
        unreachable!()
    };
    StructBuilder::new(
        fields,
        vec![
            Box::new(Float64Builder::with_capacity(capacity)) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::with_capacity(capacity)),
        ],
    )
}

fn append_coordinate(builder: &mut arrow::array::StructBuilder, coordinate: [f64; 2]) {
    builder
        .field_builder::<Float64Builder>(0)
        .expect("canonical x builder")
        .append_value(coordinate[0]);
    builder
        .field_builder::<Float64Builder>(1)
        .expect("canonical y builder")
        .append_value(coordinate[1]);
    builder.append(true);
}

#[allow(
    clippy::too_many_lines,
    reason = "six canonical GeoArrow nesting shapes share one exhaustive writer"
)]
fn build_spatial_array<R: PropRowLike>(
    name: &str,
    spatial_type: &SpatialType,
    rows: &[R],
) -> ArrayRef {
    use arrow::array::ListBuilder;

    match spatial_type.geometry {
        SpatialGeometryType::Point => {
            let mut builder = coordinate_builder(rows.len());
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::Point(coordinate),
                        ..
                    })) if observed == spatial_type => {
                        append_coordinate(&mut builder, *coordinate);
                    }
                    _ => builder.append(false),
                }
            }
            Arc::new(builder.finish())
        }
        SpatialGeometryType::LineString | SpatialGeometryType::MultiPoint => {
            let child_name = if spatial_type.geometry == SpatialGeometryType::LineString {
                "vertices"
            } else {
                "points"
            };
            let mut builder =
                ListBuilder::new(coordinate_builder(0)).with_field(Arc::new(Field::new(
                    child_name,
                    spatial_data_type(&SpatialType {
                        geometry: SpatialGeometryType::Point,
                        crs: spatial_type.crs.clone(),
                    }),
                    false,
                )));
            for row in rows {
                let coordinates = match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::LineString(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::MultiPoint(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    _ => None,
                };
                if let Some(coordinates) = coordinates {
                    for coordinate in coordinates {
                        append_coordinate(builder.values(), *coordinate);
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        SpatialGeometryType::Polygon | SpatialGeometryType::MultiLineString => {
            let coordinate_type = spatial_data_type(&SpatialType {
                geometry: SpatialGeometryType::Point,
                crs: spatial_type.crs.clone(),
            });
            let inner = ListBuilder::new(coordinate_builder(0)).with_field(Arc::new(Field::new(
                "vertices",
                coordinate_type.clone(),
                false,
            )));
            let outer_name = if spatial_type.geometry == SpatialGeometryType::Polygon {
                "rings"
            } else {
                "linestrings"
            };
            let mut builder = ListBuilder::new(inner).with_field(Arc::new(Field::new(
                outer_name,
                DataType::List(Arc::new(Field::new("vertices", coordinate_type, false))),
                false,
            )));
            for row in rows {
                let parts = match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::Polygon(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::MultiLineString(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    _ => None,
                };
                if let Some(parts) = parts {
                    for coordinates in parts {
                        for coordinate in coordinates {
                            append_coordinate(builder.values().values(), *coordinate);
                        }
                        builder.values().append(true);
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        SpatialGeometryType::MultiPolygon => {
            let coordinate_type = spatial_data_type(&SpatialType {
                geometry: SpatialGeometryType::Point,
                crs: spatial_type.crs.clone(),
            });
            let vertices = ListBuilder::new(coordinate_builder(0)).with_field(Arc::new(
                Field::new("vertices", coordinate_type.clone(), false),
            ));
            let rings_type = DataType::List(Arc::new(Field::new(
                "vertices",
                coordinate_type.clone(),
                false,
            )));
            let rings = ListBuilder::new(vertices).with_field(Arc::new(Field::new(
                "rings",
                rings_type.clone(),
                false,
            )));
            let mut builder = ListBuilder::new(rings).with_field(Arc::new(Field::new(
                "polygons",
                DataType::List(Arc::new(Field::new("rings", rings_type, false))),
                false,
            )));
            for row in rows {
                let polygons = match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::MultiPolygon(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    _ => None,
                };
                if let Some(polygons) = polygons {
                    for rings in polygons {
                        for coordinates in rings {
                            for coordinate in coordinates {
                                append_coordinate(builder.values().values().values(), *coordinate);
                            }
                            builder.values().values().append(true);
                        }
                        builder.values().append(true);
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
    }
}

fn build_heterogeneous_scalar_array<R: PropRowLike>(name: &str, rows: &[R]) -> ArrayRef {
    use arrow::array::{BooleanBuilder, Float64Builder, Int8Builder, Int64Builder, StructArray};
    use arrow::buffer::NullBuffer;

    let mut tags = Int8Builder::new();
    let mut ints = Int64Builder::new();
    let mut floats = Float64Builder::new();
    let mut strings = StringBuilder::new();
    let mut bools = BooleanBuilder::new();
    let mut valid = Vec::with_capacity(rows.len());
    for row in rows {
        let value = row.props().get(name);
        let tag = match value {
            Some(IrLiteral::Int(value)) => {
                ints.append_value(*value);
                floats.append_null();
                strings.append_null();
                bools.append_null();
                Some(0)
            }
            Some(IrLiteral::Float(value)) => {
                ints.append_null();
                floats.append_value(*value);
                strings.append_null();
                bools.append_null();
                Some(1)
            }
            Some(IrLiteral::Str(value)) => {
                ints.append_null();
                floats.append_null();
                strings.append_value(value);
                bools.append_null();
                Some(2)
            }
            Some(IrLiteral::Bool(value)) => {
                ints.append_null();
                floats.append_null();
                strings.append_null();
                bools.append_value(*value);
                Some(3)
            }
            _ => {
                ints.append_null();
                floats.append_null();
                strings.append_null();
                bools.append_null();
                None
            }
        };
        tags.append_value(tag.unwrap_or_default());
        valid.push(tag.is_some());
    }
    Arc::new(StructArray::new(
        heterogeneous_scalar_fields(),
        vec![
            Arc::new(tags.finish()),
            Arc::new(ints.finish()),
            Arc::new(floats.finish()),
            Arc::new(strings.finish()),
            Arc::new(bools.finish()),
        ],
        Some(NullBuffer::from(valid)),
    ))
}

/// Stringify a literal for a `Utf8`-coerced (mixed-type) property column.
fn literal_to_string(lit: &IrLiteral) -> String {
    match lit {
        IrLiteral::Null => String::new(),
        IrLiteral::Bool(b) => b.to_string(),
        IrLiteral::Int(i) => i.to_string(),
        IrLiteral::Float(f) => f.to_string(),
        IrLiteral::Str(s) => s.clone(),
        IrLiteral::Uuid(bytes) => {
            let mut encoded = String::with_capacity(32);
            for byte in bytes {
                std::fmt::Write::write_fmt(&mut encoded, format_args!("{byte:02x}"))
                    .expect("writing to a String cannot fail");
            }
            encoded
        }
        // A duration in a mixed (stringified) column: a deterministic
        // months/days/seconds/nanos form (the canonical `P…` render lives in graphforge-rel).
        IrLiteral::Duration {
            months,
            days,
            seconds,
            nanos,
        } => format!("{months}mo{days}d{seconds}s{nanos}ns"),
        IrLiteral::DateTime(t) => t.to_string(),
        IrLiteral::Date(d) => d.to_string(),
        // Temporal values in a mixed (stringified) column: deterministic forms
        // (the canonical renders live in graphforge-rel).
        IrLiteral::LocalDateTime { days, nanos } => format!("{days}d{nanos}ns"),
        IrLiteral::Time(nanos) => format!("{nanos}ns"),
        IrLiteral::ZonedTime { nanos, offset } => format!("{nanos}ns{offset:+}s"),
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone,
        } => format!(
            "{days}d{nanos}ns{offset:+}s{}",
            zone.as_deref().unwrap_or("")
        ),
        IrLiteral::Spatial(value) => {
            serde_json::to_string(value).expect("canonical spatial values are serializable")
        }
        // A list in a mixed (stringified) column: a deterministic bracketed form.
        IrLiteral::List(items) => {
            let parts: Vec<String> = items.iter().map(literal_to_string).collect();
            format!("[{}]", parts.join(","))
        }
        IrLiteral::Map(entries) => {
            let parts: Vec<String> = entries
                .iter()
                .map(|(key, value)| format!("{key}:{}", literal_to_string(value)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
    }
}

/// Decode a previously-written property Parquet (the dynamic per-stem schema
/// `build_property_columns` produces) back into [`PropRow`]s, so a later flush
/// can re-run inference over `[decoded ++ new]` and merge (#733).
///
/// `node_uuid` (the join key, `FixedSizeBinary(16)`) populates `PropRow.node_uuid`;
/// every other column maps by its Arrow type to an [`IrLiteral`]. A **null** slot
/// omits the key for that row — it must never become a concrete value, or it
/// would wrongly pin a previously-all-null column's type on re-inference.
///
/// `IrLiteral` is a closed 7-variant set, so the writer only ever produces these
/// column types; an unexpected type is a defensive error.
fn decode_property_rows(batches: &[RecordBatch]) -> Result<Vec<PropRow>, GfError> {
    let mut out = Vec::new();
    for batch in batches {
        decode_property_batch(batch, NODE_PROPERTY_UUID_FIELD, |node_uuid, props| {
            out.push(PropRow { node_uuid, props });
        })?;
    }
    Ok(out)
}

/// Edge analogue of [`decode_property_rows`]: decode `edge_properties/*.parquet`
/// back into [`EdgePropRow`]s (join key `edge_uuid`) for the read-merge-rewrite
/// flush cycle.
fn decode_edge_property_rows(batches: &[RecordBatch]) -> Result<Vec<EdgePropRow>, GfError> {
    let mut out = Vec::new();
    for batch in batches {
        decode_property_batch(batch, EDGE_PROPERTY_UUID_FIELD, |edge_uuid, props| {
            out.push(EdgePropRow { edge_uuid, props });
        })?;
    }
    Ok(out)
}

/// Decode every persisted node-property row while retaining its canonical
/// [`IrLiteral`] type. This is the bounded base decoder used by authoritative
/// graph-delta replay; callers must apply their own aggregate replay budget.
pub(crate) fn read_all_node_properties(dir: &Path) -> Result<Vec<TypedPropertyRow>, GfError> {
    let mut rows = Vec::new();
    for stem in crate::catalog::list_property_stems(dir) {
        let batches = crate::catalog::read_properties(dir, &stem).map_err(pq_err)?;
        rows.extend(
            decode_property_rows(&batches)?
                .into_iter()
                .map(|row| (stem.clone(), row.node_uuid, row.props)),
        );
    }
    Ok(rows)
}

/// Edge-property analogue of [`read_all_node_properties`].
pub(crate) fn read_all_edge_properties(dir: &Path) -> Result<Vec<TypedPropertyRow>, GfError> {
    let mut rows = Vec::new();
    for stem in crate::catalog::list_edge_property_stems(dir) {
        let batches = crate::catalog::read_edge_properties(dir, &stem).map_err(pq_err)?;
        rows.extend(
            decode_edge_property_rows(&batches)?
                .into_iter()
                .map(|row| (stem.clone(), row.edge_uuid, row.props)),
        );
    }
    Ok(rows)
}

/// Read the non-null property keys currently stored for one entity.
///
/// Used by `SET entity = map` to compute the authoritative replacement
/// complement even when the query plan projected only a subset of properties.
pub fn read_entity_property_keys(
    dir: &Path,
    stem: &str,
    uuid: &[u8; 16],
    is_edge: bool,
) -> Result<HashSet<String>, GfError> {
    let batches = if is_edge {
        crate::catalog::read_edge_properties(dir, stem)
    } else {
        crate::catalog::read_properties(dir, stem)
    }
    .map_err(pq_err)?;
    let rows = if is_edge {
        decode_edge_property_rows(&batches)?
            .into_iter()
            .map(|row| (row.edge_uuid, row.props))
            .collect::<Vec<_>>()
    } else {
        decode_property_rows(&batches)?
            .into_iter()
            .map(|row| (row.node_uuid, row.props))
            .collect::<Vec<_>>()
    };
    Ok(rows
        .into_iter()
        .find_map(|(row_uuid, props)| (row_uuid == *uuid).then(|| props.into_keys().collect()))
        .unwrap_or_default())
}

/// Read the complete non-null property map for one persisted entity.
///
/// Returns an empty map when the property file or entity row is absent.
pub fn read_entity_properties(
    dir: &Path,
    stem: &str,
    uuid: &[u8; 16],
    is_edge: bool,
) -> Result<HashMap<String, IrLiteral>, GfError> {
    let batches = if is_edge {
        crate::catalog::read_edge_properties(dir, stem)
    } else {
        crate::catalog::read_properties(dir, stem)
    }
    .map_err(pq_err)?;
    let rows = if is_edge {
        decode_edge_property_rows(&batches)?
            .into_iter()
            .map(|row| (row.edge_uuid, row.props))
            .collect::<Vec<_>>()
    } else {
        decode_property_rows(&batches)?
            .into_iter()
            .map(|row| (row.node_uuid, row.props))
            .collect::<Vec<_>>()
    };
    Ok(rows
        .into_iter()
        .find_map(|(row_uuid, props)| (row_uuid == *uuid).then_some(props))
        .unwrap_or_default())
}

/// Read every UUID-keyed node property row from one persisted stem.
pub fn read_node_property_rows(
    dir: &Path,
    stem: &str,
) -> Result<HashMap<[u8; 16], HashMap<String, IrLiteral>>, GfError> {
    let batches = crate::catalog::read_properties(dir, stem).map_err(pq_err)?;
    Ok(decode_property_rows(&batches)?
        .into_iter()
        .map(|row| (row.node_uuid, row.props))
        .collect())
}

/// Count non-null properties owned by the selected persisted entities across
/// every dynamic-schema property partition.
pub fn count_entity_properties<S: std::hash::BuildHasher>(
    dir: &Path,
    targets: &HashSet<[u8; 16], S>,
    is_edge: bool,
) -> Result<u64, GfError> {
    if targets.is_empty() {
        return Ok(0);
    }
    let mut count = 0u64;
    let stems = if is_edge {
        crate::catalog::list_edge_property_stems(dir)
    } else {
        crate::catalog::list_property_stems(dir)
    };
    for stem in stems {
        let batches = if is_edge {
            crate::catalog::read_edge_properties(dir, &stem)
        } else {
            crate::catalog::read_properties(dir, &stem)
        }
        .map_err(pq_err)?;
        if is_edge {
            for row in decode_edge_property_rows(&batches)? {
                if targets.contains(&row.edge_uuid) {
                    count += row.props.len() as u64;
                }
            }
        } else {
            for row in decode_property_rows(&batches)? {
                if targets.contains(&row.node_uuid) {
                    count += row.props.len() as u64;
                }
            }
        }
    }
    Ok(count)
}

/// Decode one property batch row-by-row, invoking `emit(uuid, props)` per row.
///
/// `uuid_field_name` is the join-key column (`node_uuid` / `edge_uuid`); every
/// other column maps by its Arrow type to an [`IrLiteral`]. A **null** slot
/// omits the key for that row — it must never become a concrete value, or it
/// would wrongly pin a previously-all-null column's type on re-inference.
#[allow(
    clippy::too_many_lines,
    reason = "one decode arm per Arrow type, plus per-shape struct dispatch; clearest inline"
)]
pub(crate) fn decode_property_batch(
    batch: &RecordBatch,
    uuid_field_name: &str,
    mut emit: impl FnMut([u8; 16], HashMap<String, IrLiteral>),
) -> Result<(), GfError> {
    use arrow::array::Array;

    let schema = batch.schema();
    let uuid_col = batch
        .column_by_name(uuid_field_name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            GfError::Storage(format!("property file missing {uuid_field_name} column"))
        })?;
    for r in 0..batch.num_rows() {
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(uuid_col.value(r));
        let mut props: HashMap<String, IrLiteral> = HashMap::new();
        for (c, field) in schema.fields().iter().enumerate() {
            if field.name() == uuid_field_name {
                continue;
            }
            let col = batch.column(c);
            if col.is_null(r) {
                continue; // null slot: omit the key (never fabricate a value)
            }
            let lit = decode_value(col, field, r)?;
            props.insert(field.name().clone(), lit);
        }
        emit(uuid, props);
    }
    Ok(())
}

/// Decode one property value at row `r` from its column, dispatched by Arrow
/// type (structs by field NAMES). A `List` recurses element-wise on the inner
/// field's type, so list-of-temporals reuse the struct dispatch (#1006). Shared
/// by [`decode_property_batch`] and its own list arm.
#[allow(
    clippy::too_many_lines,
    reason = "one decode arm per Arrow type, plus per-shape struct dispatch; clearest inline"
)]
fn decode_value(
    col: &arrow::array::ArrayRef,
    field: &arrow::datatypes::Field,
    r: usize,
) -> Result<IrLiteral, GfError> {
    use arrow::array::{
        Array, BooleanArray, Int32Array, Int64Array, ListArray, StructArray, Time64NanosecondArray,
    };
    if field.metadata().contains_key("ARROW:extension:name") {
        return decode_spatial_value(col, field, r);
    }
    Ok(match field.data_type() {
        DataType::Int64 => IrLiteral::Int(downcast::<Int64Array>(col, field)?.value(r)),
        DataType::Float64 => IrLiteral::Float(downcast::<Float64Array>(col, field)?.value(r)),
        DataType::Boolean => IrLiteral::Bool(downcast::<BooleanArray>(col, field)?.value(r)),
        DataType::Utf8 => IrLiteral::Str(downcast::<StringArray>(col, field)?.value(r).to_owned()),
        // A typed temporal struct (#920). Dispatch by the struct's field
        // NAMES — every persisted Struct used to be assumed a duration,
        // but `localdatetime` (and later `time`/`datetime`) are also
        // structs, so the shape must select the decode.
        DataType::Struct(fields) => {
            let s = downcast::<StructArray>(col, field)?;
            let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            match names.as_slice() {
                [
                    "__het_tag",
                    "__het_int",
                    "__het_float",
                    "__het_str",
                    "__het_bool",
                ] => {
                    let tag = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .ok_or_else(|| GfError::Storage("heterogeneous tag not Int8".into()))?
                        .value(r);
                    match tag {
                        0 => IrLiteral::Int(
                            s.column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .ok_or_else(|| {
                                    GfError::Storage("heterogeneous int not Int64".into())
                                })?
                                .value(r),
                        ),
                        1 => IrLiteral::Float(
                            s.column(2)
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .ok_or_else(|| {
                                    GfError::Storage("heterogeneous float not Float64".into())
                                })?
                                .value(r),
                        ),
                        2 => IrLiteral::Str(
                            s.column(3)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .ok_or_else(|| {
                                    GfError::Storage("heterogeneous string not Utf8".into())
                                })?
                                .value(r)
                                .to_owned(),
                        ),
                        3 => IrLiteral::Bool(
                            s.column(4)
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| {
                                    GfError::Storage("heterogeneous bool not Boolean".into())
                                })?
                                .value(r),
                        ),
                        _ => {
                            return Err(GfError::Storage(format!(
                                "unsupported heterogeneous property tag {tag}"
                            )));
                        }
                    }
                }
                ["months", "days", "seconds", "nanos"] => {
                    let i64_at = |idx: usize| -> Result<i64, GfError> {
                        Ok(s.column(idx)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                GfError::Storage("duration struct child not Int64".into())
                            })?
                            .value(r))
                    };
                    IrLiteral::Duration {
                        months: i64_at(0)?,
                        days: i64_at(1)?,
                        seconds: i64_at(2)?,
                        nanos: i64_at(3)?,
                    }
                }
                ["epoch_day"] => IrLiteral::Date(
                    s.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| GfError::Storage("date epoch_day not Int64".into()))?
                        .value(r),
                ),
                ["date", "time"] => {
                    let days = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| GfError::Storage("localdatetime date not Int64".into()))?
                        .value(r);
                    let nanos = s
                        .column(1)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| {
                            GfError::Storage("localdatetime time not Time64(ns)".into())
                        })?
                        .value(r);
                    IrLiteral::LocalDateTime { days, nanos }
                }
                ["time", "offset"] => {
                    let nanos = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| GfError::Storage("time not Time64(ns)".into()))?
                        .value(r);
                    let offset = s
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| GfError::Storage("time offset not Int32".into()))?
                        .value(r);
                    IrLiteral::ZonedTime { nanos, offset }
                }
                ["date", "time", "offset", "zone"] => {
                    let days = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| GfError::Storage("datetime date not Int64".into()))?
                        .value(r);
                    let nanos = s
                        .column(1)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| GfError::Storage("datetime time not Time64(ns)".into()))?
                        .value(r);
                    let offset = s
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| GfError::Storage("datetime offset not Int32".into()))?
                        .value(r);
                    let zone_col = s
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| GfError::Storage("datetime zone not Utf8".into()))?;
                    // A NULL zone child means offset-only (no named zone).
                    let zone = (!zone_col.is_null(r)).then(|| zone_col.value(r).to_owned());
                    IrLiteral::ZonedDateTime {
                        days,
                        nanos,
                        offset,
                        zone,
                    }
                }
                _ => {
                    return Err(GfError::Storage(format!(
                        "property column {} has unsupported struct shape {names:?}",
                        field.name()
                    )));
                }
            }
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            IrLiteral::Time(downcast::<Time64NanosecondArray>(col, field)?.value(r))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            IrLiteral::DateTime(downcast::<TimestampMicrosecondArray>(col, field)?.value(r))
        }
        // A homogeneous list (#1006): decode each element by recursing on
        // the inner field's type (so list-of-temporals reuse the struct
        // dispatch). Nested lists recurse naturally.
        DataType::List(inner) => {
            let larr = downcast::<ListArray>(col, field)?;
            let elems = larr.value(r);
            let mut items = Vec::with_capacity(elems.len());
            for j in 0..elems.len() {
                if elems.is_null(j) {
                    items.push(IrLiteral::Null);
                } else {
                    items.push(decode_value(&elems, inner, j)?);
                }
            }
            IrLiteral::List(items)
        }
        other => {
            return Err(GfError::Storage(format!(
                "property column {} has unsupported type {other:?}",
                field.name()
            )));
        }
    })
}

fn decode_spatial_value(col: &ArrayRef, field: &Field, row: usize) -> Result<IrLiteral, GfError> {
    decode_spatial_property_value(col.as_ref(), field, row).map(IrLiteral::Spatial)
}

/// Decode one canonical GeoArrow property row without reconstructing geometry
/// in a language binding.
#[allow(
    clippy::too_many_lines,
    reason = "six canonical GeoArrow nesting shapes share one exhaustive decoder"
)]
pub fn decode_spatial_property_value(
    col: &dyn arrow::array::Array,
    field: &Field,
    row: usize,
) -> Result<SpatialValue, GfError> {
    use arrow::array::{Array, ListArray, StructArray};
    let extension_name = field
        .metadata()
        .get("ARROW:extension:name")
        .ok_or_else(|| GfError::Storage("spatial field missing extension name".into()))?;
    let geometry = match extension_name.as_str() {
        "geoarrow.point" => SpatialGeometryType::Point,
        "geoarrow.linestring" => SpatialGeometryType::LineString,
        "geoarrow.polygon" => SpatialGeometryType::Polygon,
        "geoarrow.multipoint" => SpatialGeometryType::MultiPoint,
        "geoarrow.multilinestring" => SpatialGeometryType::MultiLineString,
        "geoarrow.multipolygon" => SpatialGeometryType::MultiPolygon,
        _ => [
            SpatialGeometryType::Point,
            SpatialGeometryType::LineString,
            SpatialGeometryType::Polygon,
            SpatialGeometryType::MultiPoint,
            SpatialGeometryType::MultiLineString,
            SpatialGeometryType::MultiPolygon,
        ]
        .into_iter()
        .find(|geometry| {
            spatial_data_type(&SpatialType {
                geometry: *geometry,
                crs: SpatialCrs::Epsg4326,
            }) == *field.data_type()
        })
        .ok_or_else(|| GfError::Storage("unsupported spatial extension storage type".into()))?,
    };
    let metadata = field
        .metadata()
        .get("ARROW:extension:metadata")
        .ok_or_else(|| GfError::Storage("spatial field missing extension metadata".into()))?;
    let crs_name = serde_json::from_str::<serde_json::Value>(metadata)
        .ok()
        .and_then(|value| value.get("crs")?.as_str().map(ToOwned::to_owned))
        .ok_or_else(|| GfError::Storage("spatial CRS metadata is malformed".into()))?;
    let crs = match crs_name.as_str() {
        "EPSG:4326" => SpatialCrs::Epsg4326,
        "EPSG:3857" => SpatialCrs::Epsg3857,
        _ => SpatialCrs::Preserved(crs_name),
    };
    let coordinates = match geometry {
        SpatialGeometryType::Point => SpatialCoordinates::Point(read_coordinate(
            col.as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| GfError::Storage("spatial point is not a struct".into()))?,
            row,
        )?),
        SpatialGeometryType::LineString | SpatialGeometryType::MultiPoint => {
            let value = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("spatial geometry is not a list".into()))?
                .value(row);
            let values =
                read_coordinates(value.as_any().downcast_ref::<StructArray>().ok_or_else(
                    || GfError::Storage("spatial coordinate payload is not a struct".into()),
                )?)?;
            if geometry == SpatialGeometryType::LineString {
                SpatialCoordinates::LineString(values)
            } else {
                SpatialCoordinates::MultiPoint(values)
            }
        }
        SpatialGeometryType::Polygon | SpatialGeometryType::MultiLineString => {
            let value = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("spatial geometry is not a nested list".into()))?
                .value(row);
            let lists = value
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("spatial parts are not lists".into()))?;
            let mut parts = Vec::with_capacity(lists.len());
            for index in 0..lists.len() {
                let coordinates = lists.value(index);
                parts.push(read_coordinates(
                    coordinates
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            GfError::Storage("spatial coordinates are not structs".into())
                        })?,
                )?);
            }
            if geometry == SpatialGeometryType::Polygon {
                SpatialCoordinates::Polygon(parts)
            } else {
                SpatialCoordinates::MultiLineString(parts)
            }
        }
        SpatialGeometryType::MultiPolygon => {
            let value = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("multipolygon is not a nested list".into()))?
                .value(row);
            let polygons = value
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("multipolygon polygons are not lists".into()))?;
            let mut decoded = Vec::with_capacity(polygons.len());
            for polygon_index in 0..polygons.len() {
                let polygon = polygons.value(polygon_index);
                let rings = polygon
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| GfError::Storage("multipolygon rings are not lists".into()))?;
                let mut decoded_rings = Vec::with_capacity(rings.len());
                for ring_index in 0..rings.len() {
                    let coordinates = rings.value(ring_index);
                    decoded_rings.push(read_coordinates(
                        coordinates
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .ok_or_else(|| {
                                GfError::Storage("multipolygon coordinates are not structs".into())
                            })?,
                    )?);
                }
                decoded.push(decoded_rings);
            }
            SpatialCoordinates::MultiPolygon(decoded)
        }
    };
    let preserved_crs = matches!(crs, SpatialCrs::Preserved(_));
    Ok(SpatialValue {
        spatial_type: SpatialType { geometry, crs },
        coordinates,
        extension_name: (!matches!(
            extension_name.as_str(),
            "geoarrow.point"
                | "geoarrow.linestring"
                | "geoarrow.polygon"
                | "geoarrow.multipoint"
                | "geoarrow.multilinestring"
                | "geoarrow.multipolygon"
        ))
        .then(|| extension_name.clone()),
        extension_metadata: preserved_crs.then(|| metadata.clone()),
    })
}

fn read_coordinates(array: &arrow::array::StructArray) -> Result<Vec<[f64; 2]>, GfError> {
    use arrow::array::Array;
    (0..array.len())
        .map(|index| read_coordinate(array, index))
        .collect()
}

fn read_coordinate(array: &arrow::array::StructArray, row: usize) -> Result<[f64; 2], GfError> {
    let x = array
        .column_by_name("x")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| GfError::Storage("spatial x coordinate is not Float64".into()))?;
    let y = array
        .column_by_name("y")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| GfError::Storage("spatial y coordinate is not Float64".into()))?;
    Ok([x.value(row), y.value(row)])
}

/// Downcast a column to a concrete Arrow array type, erroring with the column
/// name if the dynamic type does not match its declared field type.
fn downcast<'a, A: 'static>(
    col: &'a arrow::array::ArrayRef,
    field: &arrow::datatypes::Field,
) -> Result<&'a A, GfError> {
    col.as_any().downcast_ref::<A>().ok_or_else(|| {
        GfError::Storage(format!(
            "property column {} could not be read as its declared type",
            field.name()
        ))
    })
}

// ---------------------------------------------------------------------------
// SET / REMOVE property rewrite primitives (#791)
// ---------------------------------------------------------------------------
//
// These rewrite committed `properties/<stem>.parquet` / `edge_properties/
// <stem>.parquet` files, mirroring the writer's decode → mutate → re-infer →
// write cycle (see [`GraphWriter::flush_properties`]). The `stage_*` forms
// stage into a caller-owned [`RewriteBatch`] so one statement's rewrites
// across stems commit all-or-nothing (#790); the original four functions are
// stage-and-commit wrappers for single-stem callers.
//
// The execution layer accumulates per-uuid updates/removals from the matched
// rows, then calls these once per file stem. SET **merges** into a uuid's
// existing property map (overwriting same-named keys) and **inserts** a fresh
// row for a uuid that had no property row yet; REMOVE drops keys (a column that
// becomes all-absent disappears on re-inference). Both return the number of
// distinct entities whose file row was written.

/// Apply per-uuid property `updates` (SET) to the rows decoded from `existing`,
/// merging into each uuid's map and inserting a row for any uuid not present.
/// Returns the rebuilt row set and the number of distinct uuids touched.
fn apply_property_updates<R: PropRowLike>(
    mut rows: Vec<R>,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> (Vec<R>, u64) {
    // uuid → index into `rows` (first occurrence wins; the decode never emits
    // duplicate uuids, but a defensive first-wins keeps this total).
    let mut index: HashMap<[u8; 16], usize> = HashMap::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        index.entry(*row.uuid_bytes()).or_insert(i);
    }
    let mut touched = 0u64;
    for (uuid, new_props) in updates {
        if new_props.is_empty() {
            continue;
        }
        touched += 1;
        if let Some(&i) = index.get(uuid) {
            rows[i].props_mut().extend(new_props.clone());
        } else {
            index.insert(*uuid, rows.len());
            rows.push(R::from_parts(*uuid, new_props.clone()));
        }
    }
    (rows, touched)
}

/// Apply per-uuid property `removals` (REMOVE) to the rows decoded from
/// `existing`. Removing an absent key or an absent uuid is a no-op (openCypher).
/// Returns the rebuilt row set and the number of distinct uuids touched (a uuid
/// is counted even if every named key was already absent — the entity was
/// targeted).
fn apply_property_removals<R: PropRowLike>(
    mut rows: Vec<R>,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> (Vec<R>, u64) {
    let mut index: HashMap<[u8; 16], usize> = HashMap::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        index.entry(*row.uuid_bytes()).or_insert(i);
    }
    let mut touched = 0u64;
    for (uuid, keys) in removals {
        if keys.is_empty() {
            continue;
        }
        touched += 1;
        if let Some(&i) = index.get(uuid) {
            let props = rows[i].props_mut();
            for k in keys {
                props.remove(k);
            }
        }
    }
    (rows, touched)
}

/// Stage a rewrite of `properties/<stem>.parquet` applying per-`node_uuid` SET
/// `updates` into `staged` (committed by the caller, #790).
///
/// Reads the current file (decode → re-infer over the merged set), inserts a
/// row for any node that had no property row yet, and stages the rebuilt file.
/// A write is skipped only when the merged set is empty. Returns the number of
/// distinct nodes whose row was set.
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
// The execution layer always accumulates updates with the default hasher;
// generalizing the nested maps over `BuildHasher` would add two type params per
// fn for no caller benefit.
#[allow(clippy::implicit_hasher)]
pub fn stage_set_node_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let targets = updates.keys().copied().collect();
    let (mut existing, _) = crate::property_overlay::read_authenticated_property_snapshots_for(
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
        &targets,
    )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
    )?);
    let rows = existing
        .into_values()
        .map(|row| PropRow {
            node_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_updates(rows, updates);
    let rows = rows
        .into_iter()
        .filter(|row| updates.contains_key(&row.node_uuid))
        .collect::<Vec<_>>();
    stage_node_property_file(staged, dir, stem, &rows, None)?;
    Ok(touched)
}

fn pending_property_snapshots(
    staged: &RewriteBatch,
    _dir: &Path,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
) -> Result<BTreeMap<[u8; 16], crate::property_overlay::PropertySnapshotRow>, GfError> {
    Ok(staged
        .property_window_rows(kind, route)
        .cloned()
        .unwrap_or_default())
}

/// Stage a rewrite of `properties/<stem>.parquet` applying per-`node_uuid`
/// REMOVE `removals`. Returns the number of distinct nodes targeted.
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn stage_remove_node_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let targets = removals.keys().copied().collect();
    let (mut existing, _) = crate::property_overlay::read_authenticated_property_snapshots_for(
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
        &targets,
    )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
    )?);
    let rows = existing
        .into_values()
        .map(|row| PropRow {
            node_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_removals(rows, removals);
    let rows = rows
        .into_iter()
        .filter(|row| removals.contains_key(&row.node_uuid))
        .collect::<Vec<_>>();
    stage_node_property_file(staged, dir, stem, &rows, None)?;
    Ok(touched)
}

/// Stage a rewrite of `edge_properties/<rel_stem>.parquet` applying
/// per-`edge_uuid` SET `updates`. Edge analogue of
/// [`stage_set_node_properties`]; the join key is `edge_uuid` and the file is
/// routed by relation name.
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn stage_set_edge_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    rel_stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let targets = updates.keys().copied().collect();
    let (mut existing, _) = crate::property_overlay::read_authenticated_property_snapshots_for(
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        rel_stem,
        &targets,
    )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        rel_stem,
    )?);
    let rows = existing
        .into_values()
        .map(|row| EdgePropRow {
            edge_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_updates(rows, updates);
    let rows = rows
        .into_iter()
        .filter(|row| updates.contains_key(&row.edge_uuid))
        .collect::<Vec<_>>();
    stage_edge_property_file(staged, dir, rel_stem, &rows, None)?;
    Ok(touched)
}

/// Stage a rewrite of `edge_properties/<rel_stem>.parquet` applying
/// per-`edge_uuid` REMOVE `removals`. Edge analogue of
/// [`stage_remove_node_properties`].
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn stage_remove_edge_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    rel_stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let targets = removals.keys().copied().collect();
    let (mut existing, _) = crate::property_overlay::read_authenticated_property_snapshots_for(
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        rel_stem,
        &targets,
    )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        rel_stem,
    )?);
    let rows = existing
        .into_values()
        .map(|row| EdgePropRow {
            edge_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_removals(rows, removals);
    let rows = rows
        .into_iter()
        .filter(|row| removals.contains_key(&row.edge_uuid))
        .collect::<Vec<_>>();
    stage_edge_property_file(staged, dir, rel_stem, &rows, None)?;
    Ok(touched)
}

/// Rewrite `properties/<stem>.parquet` applying per-`node_uuid` SET `updates`,
/// staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn set_node_properties(
    dir: &Path,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_set_node_properties(&mut staged, dir, stem, updates)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rewrite `properties/<stem>.parquet` applying per-`node_uuid` REMOVE
/// `removals`, staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn remove_node_properties(
    dir: &Path,
    stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_remove_node_properties(&mut staged, dir, stem, removals)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rewrite `edge_properties/<rel_stem>.parquet` applying per-`edge_uuid` SET
/// `updates`, staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn set_edge_properties_rewrite(
    dir: &Path,
    rel_stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_set_edge_properties(&mut staged, dir, rel_stem, updates)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rewrite `edge_properties/<rel_stem>.parquet` applying per-`edge_uuid`
/// REMOVE `removals`, staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn remove_edge_properties(
    dir: &Path,
    rel_stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_remove_edge_properties(&mut staged, dir, rel_stem, removals)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rebuild `properties/<stem>.parquet` from `rows` and stage it. A write is
/// skipped only when `rows` is empty (an absent file with no inserts) — the
/// dynamic-schema builder cannot emit a zero-row key column, and there is
/// nothing to persist. A REMOVE that empties a row's last property keeps the
/// row (now with no property columns); its property map decodes back as empty.
fn stage_node_property_file(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    rows: &[PropRow],
    metadata: Option<&HashMap<String, String>>,
) -> Result<(), GfError> {
    if rows.is_empty() {
        return Ok(());
    }
    let (schema, cols) = build_property_columns(stem, rows)?;
    let schema = preserve_semantic_route_metadata(Arc::new(schema), metadata);
    stage_property_fragment(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
        schema,
        cols,
        false,
    )
}

/// Edge analogue of [`stage_node_property_file`] (key `edge_uuid`, file routed
/// by relation name under `edge_properties/`).
fn stage_edge_property_file(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    rows: &[EdgePropRow],
    metadata: Option<&HashMap<String, String>>,
) -> Result<(), GfError> {
    if rows.is_empty() {
        return Ok(());
    }
    let (schema, cols) =
        build_property_columns_keyed(EDGE_PROPERTY_UUID_FIELD, "graphforge.rel_type", stem, rows)?;
    let schema = preserve_semantic_route_metadata(Arc::new(schema), metadata);
    stage_property_fragment(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        stem,
        schema,
        cols,
        false,
    )
}

/// Stage a rebuilt property file under `<dir>/<subdir>/<stem>.parquet` (the
/// staging core creates the subdirectory), replacing any content this
/// statement already staged for it. Callers guard the empty-row case before
/// reaching here.
fn merge_node_property_window(rows: Vec<PropRow>) -> Vec<PropRow> {
    let mut merged = BTreeMap::<[u8; 16], HashMap<String, IrLiteral>>::new();
    for row in rows {
        merged.entry(row.node_uuid).or_default().extend(row.props);
    }
    merged
        .into_iter()
        .map(|(node_uuid, props)| PropRow { node_uuid, props })
        .collect()
}

fn merge_edge_property_window(rows: Vec<EdgePropRow>) -> Vec<EdgePropRow> {
    let mut merged = BTreeMap::<[u8; 16], HashMap<String, IrLiteral>>::new();
    for row in rows {
        merged.entry(row.edge_uuid).or_default().extend(row.props);
    }
    merged
        .into_iter()
        .map(|(edge_uuid, props)| EdgePropRow { edge_uuid, props })
        .collect()
}

pub(crate) fn stage_property_tombstones<S: std::hash::BuildHasher>(
    staged: &mut RewriteBatch,
    dir: &Path,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
    uuids: &HashSet<[u8; 16], S>,
) -> Result<(), GfError> {
    if uuids.is_empty() {
        return Ok(());
    }
    let mut uuids = uuids.iter().copied().collect::<Vec<_>>();
    uuids.sort_unstable();
    let uuid_field = match kind {
        crate::property_overlay::PropertyRouteKind::Node => NODE_PROPERTY_UUID_FIELD,
        crate::property_overlay::PropertyRouteKind::Edge => EDGE_PROPERTY_UUID_FIELD,
    };
    let route_key = match kind {
        crate::property_overlay::PropertyRouteKind::Node => "graphforge.entity_type",
        crate::property_overlay::PropertyRouteKind::Edge => "graphforge.rel_type",
    };
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new(uuid_field, DataType::FixedSizeBinary(16), false)],
        HashMap::from([(route_key.to_owned(), route.to_owned())]),
    ));
    let column = FixedSizeBinaryArray::try_from_iter(uuids.into_iter().map(|uuid| uuid.to_vec()))
        .map_err(pq_err)?;
    stage_property_fragment(
        staged,
        dir,
        kind,
        route,
        schema,
        vec![Arc::new(column)],
        true,
    )
}

fn stage_property_fragment(
    staged: &mut RewriteBatch,
    dir: &Path,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
    schema: SchemaRef,
    mut cols: Vec<ArrayRef>,
    tombstone: bool,
) -> Result<(), GfError> {
    use crate::property_overlay::PROPERTY_TOMBSTONE_FIELD;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.insert(
        1,
        Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
    );
    let rows = cols.first().map_or(0, |column| column.len());
    cols.insert(1, Arc::new(BooleanArray::from(vec![tombstone; rows])));
    let metadata = schema.metadata().clone();
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata.clone()));
    let batch = RecordBatch::try_new(schema, cols).map_err(pq_err)?;
    let uuid_field = match kind {
        crate::property_overlay::PropertyRouteKind::Node => NODE_PROPERTY_UUID_FIELD,
        crate::property_overlay::PropertyRouteKind::Edge => EDGE_PROPERTY_UUID_FIELD,
    };
    let rows = crate::property_overlay::decode_snapshot_batch(&batch, uuid_field)?;
    staged.accumulate_property_window(dir, kind, route, rows, metadata)
}

pub(crate) fn seal_property_windows(
    staged: &mut RewriteBatch,
    dir: &Path,
    generation: u64,
) -> Result<(), GfError> {
    use crate::property_overlay::{
        PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT,
        PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD,
        PropertyFragmentId, enumerate_property_fragments,
    };
    for (key, window) in staged.take_property_windows() {
        if window.project_root != dir {
            return Err(GfError::Storage(
                "property window project root changed".into(),
            ));
        }
        if enumerate_property_fragments(dir, key.kind, &key.route)?
            .iter()
            .any(|fragment| fragment.id.generation >= generation)
        {
            return Err(GfError::Storage(
                "property fragment generation is not strictly monotonic".into(),
            ));
        }
        let rows = window.rows.into_values().collect::<Vec<_>>();
        let tombstones = rows.iter().map(|row| row.tombstone).collect::<Vec<_>>();
        let batch = property_snapshots_to_batch(
            &key.route,
            matches!(key.kind, crate::property_overlay::PropertyRouteKind::Edge),
            rows,
        )?
        .ok_or_else(|| GfError::Storage("property window cannot seal empty fragment".into()))?;
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.insert(
            1,
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
        );
        let mut columns = batch.columns().to_vec();
        columns.insert(1, Arc::new(BooleanArray::from(tombstones)));
        let mut metadata = window.metadata;
        metadata.insert(
            PROPERTY_OVERLAY_FORMAT_KEY.into(),
            PROPERTY_OVERLAY_FORMAT.into(),
        );
        metadata.insert(PROPERTY_ROUTE_KEY.into(), key.route.clone());
        metadata.insert(PROPERTY_KIND_KEY.into(), key.kind.metadata_value().into());
        metadata.insert(PROPERTY_GENERATION_KEY.into(), generation.to_string());
        metadata.insert(PROPERTY_ORDINAL_KEY.into(), "0".into());
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(pq_err)?;
        let subdir = match key.kind {
            crate::property_overlay::PropertyRouteKind::Node => "properties",
            crate::property_overlay::PropertyRouteKind::Edge => "edge_properties",
        };
        staged.stage(
            &dir.join(subdir).join(&key.route).join(
                PropertyFragmentId {
                    generation,
                    ordinal: 0,
                }
                .file_name(),
            ),
            schema,
            &batch,
        )?;
    }
    Ok(())
}

/// `properties/<stem>.parquet` under `dir`.
fn node_props_path(dir: &Path, stem: &str) -> PathBuf {
    dir.join("properties").join(format!("{stem}.parquet"))
}

// ---------------------------------------------------------------------------
// Parquet write helper
// ---------------------------------------------------------------------------

use crate::staging::RewriteBatch;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use graphforge_core::uuid::new_v7;
    use tempfile::TempDir;

    const TS: i64 = 1_700_000_000_000_000;

    #[test]
    fn create_node_persists_complete_label_set_and_primary_label() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        w.create_node_with_labels(new_v7(), &[TypeId(4), TypeId(9)])
            .unwrap();
        w.flush().unwrap();

        let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
        let batch = &nodes[0];
        let primary = batch
            .column_by_name("type_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(primary.value(0), 4);
        let sets = batch
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let labels = sets.value(0);
        let labels = labels.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(labels.values(), &[4, 9]);
    }

    #[test]
    fn surrogate_ids_are_monotonic_from_one() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        assert_eq!(w.create_node(new_v7(), TypeId(0)).unwrap(), 1);
        assert_eq!(w.create_node(new_v7(), TypeId(0)).unwrap(), 2);
        assert_eq!(w.create_node(new_v7(), TypeId(0)).unwrap(), 3);
    }

    #[test]
    fn reopen_recovers_surrogate_tails_without_full_topology_reads() {
        let dir = TempDir::new().unwrap();
        let first_node = new_v7();
        let second_node = new_v7();
        let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        assert_eq!(first.create_node(first_node, TypeId(0)).unwrap(), 1);
        assert_eq!(first.create_node(second_node, TypeId(0)).unwrap(), 2);
        assert_eq!(
            first
                .create_edge(new_v7(), "KNOWS", &first_node, &second_node)
                .unwrap(),
            1
        );
        first.flush().unwrap();

        let _measurement = crate::io_stats::test_measurement_guard();
        crate::io_stats::reset();
        let mut reopened = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        assert_eq!(reopened.create_node(new_v7(), TypeId(0)).unwrap(), 3);
        reopened.register_existing_node(first_node, 1);
        reopened.register_existing_node(second_node, 2);
        assert_eq!(
            reopened
                .create_edge(new_v7(), "KNOWS", &first_node, &second_node)
                .unwrap(),
            2
        );
        let io = crate::io_stats::snapshot();
        assert_eq!(
            io.node_full_reads, 0,
            "writer reopen must use bounded tails"
        );
        assert_eq!(
            io.edge_full_reads, 0,
            "writer reopen must use bounded tails"
        );
    }

    #[test]
    fn streaming_node_append_normalizes_legacy_scalar_labels() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("topology")).unwrap();
        let legacy_schema = Arc::new(Schema::new(vec![
            uuid_field("node_uuid"),
            crate::schemas::id_field("node_id"),
            Field::new("type_id", DataType::UInt32, false),
            crate::schemas::ts_field("created_at"),
            crate::schemas::ts_field("updated_at"),
        ]));
        let uuid = FixedSizeBinaryArray::try_from_iter([vec![1_u8; 16]].into_iter()).unwrap();
        let ts = TimestampMicrosecondArray::from(vec![TS])
            .with_timezone_opt(Some(Arc::<str>::from("UTC")));
        let legacy = RecordBatch::try_new(
            legacy_schema,
            vec![
                Arc::new(uuid),
                Arc::new(UInt64Array::from(vec![1_u64])),
                Arc::new(UInt32Array::from(vec![7_u32])),
                Arc::new(ts.clone()),
                Arc::new(ts),
            ],
        )
        .unwrap();
        let file = File::create(dir.path().join("topology/nodes.parquet")).unwrap();
        let mut parquet =
            parquet::arrow::ArrowWriter::try_new(file, legacy.schema(), None).unwrap();
        parquet.write(&legacy).unwrap();
        parquet.close().unwrap();

        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        assert_eq!(writer.create_node(new_v7(), TypeId(9)).unwrap(), 2);
        writer.flush().unwrap();

        let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
        assert_eq!(nodes.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let labels = nodes[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let first = labels.value(0);
        let first = first.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(first.values(), &[7]);
    }

    #[test]
    fn create_edge_with_unknown_endpoint_errors() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        let a = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        // `b` was never created.
        let b = new_v7();
        let e = w.create_edge(new_v7(), "KNOWS", &a, &b);
        assert!(matches!(e, Err(GfError::Storage(_))), "got {e:?}");

        let unknown_source = new_v7();
        let source_error = w.create_edge(new_v7(), "KNOWS", &unknown_source, &a);
        assert!(matches!(&source_error, Err(GfError::Storage(_))));
        assert!(source_error.unwrap_err().to_string().contains("source"));
    }

    #[test]
    fn register_existing_node_resolves_edge_endpoint() {
        // #703: a MATCH-bound node is referenced (not created). Registering its
        // identity lets a subsequent edge resolve it without writing a node row
        // or advancing the surrogate counter.
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        // `a` already exists on disk with surrogate 42 (e.g. from a prior write).
        let a = new_v7();
        w.register_existing_node(a, 42);
        // A freshly-minted `b` (next surrogate is 1 — register did not advance it).
        let b = new_v7();
        assert_eq!(w.create_node(b, TypeId(0)).unwrap(), 1);
        // The edge resolves both endpoints; its src_id is the registered 42.
        let edge_id = w
            .create_edge(new_v7(), "KNOWS", &a, &b)
            .expect("edge with a registered endpoint resolves");
        assert_eq!(edge_id, 1);
    }

    #[test]
    fn register_existing_node_does_not_write_a_node_row() {
        // Registering an existing node must not buffer a NodeRow — only the one
        // genuinely-created node is flushed.
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        w.register_existing_node(new_v7(), 7);
        let created = new_v7();
        w.create_node(created, TypeId(0)).unwrap();
        w.flush().unwrap();
        // Exactly one node on disk (the created one), not two.
        let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
        let rows: usize = nodes.iter().map(arrow::array::RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 1,
            "register_existing_node must not persist a node row"
        );
    }

    #[test]
    fn empty_flush_creates_no_directories() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        w.flush().unwrap();
        assert!(!dir.path().join("topology").exists());
        assert!(!dir.path().join("properties").exists());
    }

    #[test]
    fn null_first_property_column_infers_later_concrete_type() {
        use arrow::array::Array;
        use arrow::datatypes::DataType;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        let b = new_v7();
        let c = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_node(c, TypeId(0)).unwrap();
        // First row: `score` is Null. Later rows: consistently Int.
        w.set_properties(
            &a,
            None,
            HashMap::from([("score".to_owned(), IrLiteral::Null)]),
        )
        .unwrap();
        w.set_properties(
            &b,
            None,
            HashMap::from([("score".to_owned(), IrLiteral::Int(10))]),
        )
        .unwrap();
        w.set_properties(
            &c,
            None,
            HashMap::from([("score".to_owned(), IrLiteral::Int(20))]),
        )
        .unwrap();
        w.flush().unwrap();

        let path = dir.path().join("properties").join("_untyped.parquet");
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema().clone();
        // Exactly one `score` column (no duplicate from the null row), typed Int64.
        let score_fields: Vec<_> = schema
            .fields()
            .iter()
            .filter(|f| f.name() == "score")
            .collect();
        assert_eq!(score_fields.len(), 1, "expected a single score column");
        assert_eq!(
            score_fields[0].data_type(),
            &DataType::Int64,
            "null-first then Int should infer Int64, not Utf8"
        );

        // Round-trip: 3 rows, first null then 10, 20.
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let scores = batch
            .column(schema.index_of("score").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(scores.is_null(0));
        assert_eq!(scores.value(1), 10);
        assert_eq!(scores.value(2), 20);
    }

    #[test]
    fn mixed_type_property_column_uses_tagged_scalars() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        let b = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("x".to_owned(), IrLiteral::Int(1))]),
        )
        .unwrap();
        w.set_properties(
            &b,
            None,
            HashMap::from([("x".to_owned(), IrLiteral::Str("two".to_owned()))]),
        )
        .unwrap();
        w.flush().unwrap();

        let path = dir.path().join("properties").join("_untyped.parquet");
        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema().clone();
        let x = schema.field_with_name("x").unwrap();
        assert_eq!(
            x.data_type(),
            &DataType::Struct(heterogeneous_scalar_fields())
        );
    }

    #[test]
    fn every_heterogeneous_scalar_tag_round_trips_exactly() {
        let dir = TempDir::new().unwrap();
        let cases = [
            IrLiteral::Int(-1),
            IrLiteral::Float(2.25),
            IrLiteral::Str("three".into()),
            IrLiteral::Bool(true),
        ];
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let mut expected = HashMap::new();
        for value in cases {
            let node = new_v7();
            writer.create_node(node, TypeId(0)).unwrap();
            writer
                .set_properties(
                    &node,
                    None,
                    HashMap::from([("mixed".into(), value.clone())]),
                )
                .unwrap();
            expected.insert(to_bytes(&node), value);
        }
        writer.flush().unwrap();

        let reopened = read_node_props(dir.path(), "_untyped");
        assert_eq!(reopened.len(), expected.len());
        for (node, value) in expected {
            assert_eq!(reopened[&node].get("mixed"), Some(&value));
        }
    }

    // -----------------------------------------------------------------------
    // SET / REMOVE rewrite primitives (#791)
    // -----------------------------------------------------------------------

    /// Read a node-property file back into a `uuid → props` map (mirrors the
    /// decode the rewrite primitives use), for assertions.
    fn read_node_props(dir: &Path, stem: &str) -> HashMap<[u8; 16], HashMap<String, IrLiteral>> {
        read_node_property_rows(dir, stem).unwrap()
    }

    fn read_edge_props(dir: &Path, stem: &str) -> HashMap<[u8; 16], HashMap<String, IrLiteral>> {
        let batches = crate::catalog::read_edge_properties(dir, stem).unwrap();
        let mut out = HashMap::new();
        for row in decode_edge_property_rows(&batches).unwrap() {
            out.insert(row.edge_uuid, row.props);
        }
        out
    }

    #[test]
    fn set_node_properties_sets_new_and_overwrites_existing() {
        let dir = TempDir::new().unwrap();
        assert!(
            read_node_property_rows(dir.path(), "_untyped")
                .unwrap()
                .is_empty()
        );
        fs::create_dir_all(dir.path().join("properties")).unwrap();
        fs::write(dir.path().join("properties/_untyped.parquet"), b"invalid").unwrap();
        assert!(read_node_property_rows(dir.path(), "_untyped").is_err());
        fs::remove_file(dir.path().join("properties/_untyped.parquet")).unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
        )
        .unwrap();
        w.flush().unwrap();

        let ab = to_bytes(&a);
        let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
        // Overwrite `age` and add a new `name`.
        let updates = HashMap::from([(
            ab,
            HashMap::from([
                ("age".to_owned(), IrLiteral::Int(31)),
                ("name".to_owned(), IrLiteral::Str("Al".to_owned())),
            ]),
        )]);
        let touched = set_node_properties(dir.path(), "_untyped", &updates).unwrap();
        assert_eq!(touched, 1);
        assert_eq!(
            crate::generation::read_search_generation(dir.path()).unwrap(),
            search_generation + 1
        );

        let props = read_node_props(dir.path(), "_untyped");
        assert_eq!(props[&ab]["age"], IrLiteral::Int(31));
        assert_eq!(props[&ab]["name"], IrLiteral::Str("Al".to_owned()));
    }

    #[test]
    fn set_node_properties_inserts_row_for_propertyless_node() {
        // A node with no property row yet must get a fresh row on SET.
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.flush().unwrap(); // no properties written → no _untyped file

        let ab = to_bytes(&a);
        let updates =
            HashMap::from([(ab, HashMap::from([("age".to_owned(), IrLiteral::Int(42))]))]);
        let touched = set_node_properties(dir.path(), "_untyped", &updates).unwrap();
        assert_eq!(touched, 1);

        let props = read_node_props(dir.path(), "_untyped");
        assert_eq!(props[&ab]["age"], IrLiteral::Int(42));
    }

    #[test]
    fn set_node_properties_routes_by_stem_in_strict_mode() {
        // Strict/Advisory route to properties/<Entity>.parquet, not _untyped.
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        let a = new_v7();
        w.create_node(a, TypeId(1)).unwrap();
        w.set_properties(
            &a,
            Some("Person"),
            HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
        )
        .unwrap();
        w.flush().unwrap();

        let ab = to_bytes(&a);
        let updates =
            HashMap::from([(ab, HashMap::from([("age".to_owned(), IrLiteral::Int(99))]))]);
        set_node_properties(dir.path(), "Person", &updates).unwrap();

        assert!(
            dir.path()
                .join("properties")
                .join("Person.parquet")
                .exists()
        );
        let props = read_node_props(dir.path(), "Person");
        assert_eq!(props[&ab]["age"], IrLiteral::Int(99));
    }

    #[test]
    fn remove_node_properties_drops_key_and_column_when_last() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
        )
        .unwrap();
        w.flush().unwrap();

        let ab = to_bytes(&a);
        let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
        let removals = HashMap::from([(ab, HashSet::from(["age".to_owned()]))]);
        let touched = remove_node_properties(dir.path(), "_untyped", &removals).unwrap();
        assert_eq!(touched, 1);
        assert_eq!(
            crate::generation::read_search_generation(dir.path()).unwrap(),
            search_generation + 1
        );

        // The only property was removed → the row's map is empty and the `age`
        // column is gone from the re-inferred schema.
        let props = read_node_props(dir.path(), "_untyped");
        assert!(props.get(&ab).map_or(true, HashMap::is_empty));
    }

    #[test]
    fn remove_node_properties_missing_key_and_uuid_are_noops() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
        )
        .unwrap();
        w.flush().unwrap();

        let ab = to_bytes(&a);
        // Remove a key that isn't there, plus a uuid that doesn't exist.
        let removals = HashMap::from([
            (ab, HashSet::from(["nope".to_owned()])),
            (to_bytes(&new_v7()), HashSet::from(["age".to_owned()])),
        ]);
        remove_node_properties(dir.path(), "_untyped", &removals).unwrap();

        // `age` survives untouched.
        let props = read_node_props(dir.path(), "_untyped");
        assert_eq!(props[&ab]["age"], IrLiteral::Int(30));
    }

    #[test]
    fn set_and_remove_edge_properties_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let a = new_v7();
        let b = new_v7();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        let e = new_v7();
        w.create_edge(e, "KNOWS", &a, &b).unwrap();
        w.set_edge_properties(
            &e,
            Some("KNOWS"),
            HashMap::from([("since".to_owned(), IrLiteral::Int(2019))]),
        )
        .unwrap();
        w.flush().unwrap();

        let eb = to_bytes(&e);
        let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
        // SET overwrites since.
        let updates = HashMap::from([(
            eb,
            HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
        )]);
        assert_eq!(
            set_edge_properties_rewrite(dir.path(), "KNOWS", &updates).unwrap(),
            1
        );
        assert_eq!(
            read_edge_props(dir.path(), "KNOWS")[&eb]["since"],
            IrLiteral::Int(2020)
        );

        // REMOVE since.
        let removals = HashMap::from([(eb, HashSet::from(["since".to_owned()]))]);
        assert_eq!(
            remove_edge_properties(dir.path(), "KNOWS", &removals).unwrap(),
            1
        );
        let props = read_edge_props(dir.path(), "KNOWS");
        assert!(props.get(&eb).map_or(true, HashMap::is_empty));
        assert_eq!(
            crate::generation::read_search_generation(dir.path()).unwrap(),
            search_generation
        );
    }

    #[test]
    fn set_node_properties_empty_map_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(new_v7(), TypeId(0)).unwrap();
        w.flush().unwrap();

        let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
        let touched = set_node_properties(dir.path(), "_untyped", &HashMap::new()).unwrap();
        assert_eq!(touched, 0);
        assert_eq!(
            crate::generation::read_search_generation(dir.path()).unwrap(),
            search_generation
        );
        // No property file was created from an empty update set.
        assert!(
            !dir.path()
                .join("properties")
                .join("_untyped.parquet")
                .exists()
        );
    }

    #[test]
    fn staged_set_is_invisible_until_commit_across_stems() {
        // One RewriteBatch spanning a node-property stem AND an edge-property
        // stem (#790): nothing changes until commit, then both apply at once.
        let dir = TempDir::new().unwrap();
        let (a, e) = (new_v7(), new_v7());
        let b = new_v7();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_edge(e, "KNOWS", &a, &b).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("old".into()))]),
        )
        .unwrap();
        w.set_edge_properties(
            &e,
            Some("KNOWS"),
            HashMap::from([("since".to_owned(), IrLiteral::Int(2000))]),
        )
        .unwrap();
        w.flush().unwrap();

        let (ab, eb) = (to_bytes(&a), to_bytes(&e));
        let node_updates = HashMap::from([(
            ab,
            HashMap::from([("name".to_owned(), IrLiteral::Str("new".into()))]),
        )]);
        let edge_updates = HashMap::from([(
            eb,
            HashMap::from([("since".to_owned(), IrLiteral::Int(2024))]),
        )]);

        let mut staged = RewriteBatch::new();
        let touched = stage_set_node_properties(&mut staged, dir.path(), "_untyped", &node_updates)
            .unwrap()
            + stage_set_edge_properties(&mut staged, dir.path(), "KNOWS", &edge_updates).unwrap();
        assert_eq!(touched, 2, "one node + one edge written");

        // Invisible while staged.
        assert_eq!(
            read_node_props(dir.path(), "_untyped")[&ab]["name"],
            IrLiteral::Str("old".into())
        );
        assert_eq!(
            read_edge_props(dir.path(), "KNOWS")[&eb]["since"],
            IrLiteral::Int(2000)
        );

        staged.commit().unwrap();
        assert_eq!(
            read_node_props(dir.path(), "_untyped")[&ab]["name"],
            IrLiteral::Str("new".into())
        );
        assert_eq!(
            read_edge_props(dir.path(), "KNOWS")[&eb]["since"],
            IrLiteral::Int(2024)
        );
    }

    #[test]
    fn same_window_set_then_remove_seals_one_ordered_snapshot() {
        let dir = TempDir::new().unwrap();
        let node = new_v7();
        let uuid = to_bytes(&node);
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        writer.create_node(node, TypeId(0)).unwrap();
        writer
            .set_properties(
                &node,
                None,
                HashMap::from([("base".into(), IrLiteral::Int(1))]),
            )
            .unwrap();
        writer.flush().unwrap();
        let prior_property_generation =
            crate::generation::read_property_generation(dir.path()).unwrap();

        let mut staged = RewriteBatch::new();
        stage_set_node_properties(
            &mut staged,
            dir.path(),
            "_untyped",
            &HashMap::from([(
                uuid,
                HashMap::from([
                    ("keep".into(), IrLiteral::Int(2)),
                    ("remove".into(), IrLiteral::Int(3)),
                ]),
            )]),
        )
        .unwrap();
        stage_remove_node_properties(
            &mut staged,
            dir.path(),
            "_untyped",
            &HashMap::from([(uuid, HashSet::from(["remove".into()]))]),
        )
        .unwrap();
        assert_eq!(staged.property_window_count(), 1);
        staged.commit().unwrap();
        assert_eq!(
            crate::generation::read_property_generation(dir.path()).unwrap(),
            prior_property_generation + 1
        );
        let properties = read_entity_properties(dir.path(), "_untyped", &uuid, false).unwrap();
        assert_eq!(properties.get("base"), Some(&IrLiteral::Int(1)));
        assert_eq!(properties.get("keep"), Some(&IrLiteral::Int(2)));
        assert!(!properties.contains_key("remove"));
    }

    // -----------------------------------------------------------------------
    // Pending-buffer API (#792)
    // -----------------------------------------------------------------------

    #[test]
    fn pending_nodes_batch_is_canonical_and_non_consuming() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let empty = w.pending_nodes_batch().unwrap();
        assert_eq!(empty.schema(), TOPOLOGY_NODES_SCHEMA.clone());
        assert_eq!(empty.num_rows(), 0);

        let node = new_v7();
        w.create_node_with_labels(node, &[TypeId(3), TypeId(7)])
            .unwrap();
        for _ in 0..2 {
            let batch = w.pending_nodes_batch().unwrap();
            assert_eq!(batch.schema(), TOPOLOGY_NODES_SCHEMA.clone());
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .column_by_name("node_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                1
            );
        }
        assert!(w.contains_pending_node(&to_bytes(&node)));
    }

    #[test]
    fn cancel_nodes_drops_rows_props_and_mapping() {
        let dir = TempDir::new().unwrap();
        let (a, b) = (new_v7(), new_v7());
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
        )
        .unwrap();

        assert!(w.contains_pending_node(&to_bytes(&a)));
        let dropped = w.cancel_nodes(&HashSet::from([to_bytes(&a)]));
        assert_eq!(dropped, 1);
        assert!(!w.contains_pending_node(&to_bytes(&a)));

        // The mapping is forgotten: an edge to the cancelled node must fail.
        let err = w.create_edge(new_v7(), "KNOWS", &b, &a);
        assert!(err.is_err(), "edge to a cancelled node must fail");

        w.flush().unwrap();
        // Only b hit disk; a's property row never did.
        let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
        let total: usize = nodes.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "only b persisted");
        assert!(
            !read_node_props(dir.path(), "_untyped").contains_key(&to_bytes(&a)),
            "cancelled node's props never hit disk"
        );
    }

    #[test]
    fn cancel_edges_drops_rows_and_edge_props() {
        let dir = TempDir::new().unwrap();
        let (a, b) = (new_v7(), new_v7());
        let (e1, e2) = (new_v7(), new_v7());
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_edge(e1, "KNOWS", &a, &b).unwrap();
        w.create_edge(e2, "KNOWS", &b, &a).unwrap();
        w.set_edge_properties(
            &e1,
            Some("KNOWS"),
            HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
        )
        .unwrap();

        assert!(w.contains_pending_edge(&to_bytes(&e1)));
        assert_eq!(w.cancel_edges(&HashSet::from([to_bytes(&e1)])), 1);
        assert!(!w.contains_pending_edge(&to_bytes(&e1)));

        w.flush().unwrap();
        assert!(
            !read_edge_props(dir.path(), "KNOWS").contains_key(&to_bytes(&e1)),
            "cancelled edge's props never hit disk"
        );
        let edges = crate::catalog::read_parquet_or_empty(
            &dir.path().join("topology/edges/_exploratory.parquet"),
            EXPLORATORY_EDGE_SCHEMA.clone(),
        )
        .unwrap();
        let total: usize = edges.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "only e2 persisted");
    }

    #[test]
    fn pending_incident_edge_uuids_sees_buffered_edges() {
        let dir = TempDir::new().unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let e_ab = new_v7();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_node(c, TypeId(0)).unwrap();
        w.create_edge(e_ab, "KNOWS", &a, &b).unwrap();

        // Incident from either endpoint; c has none.
        let hits = w.pending_incident_edge_uuids(&HashSet::from([to_bytes(&b)]));
        assert_eq!(hits, vec![to_bytes(&e_ab)]);
        assert!(
            w.pending_incident_edge_uuids(&HashSet::from([to_bytes(&c)]))
                .is_empty()
        );
    }

    #[test]
    fn pending_query_and_label_edits_are_exact_before_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let (alice, bob, edge) = (new_v7(), new_v7(), new_v7());
        let (alice_bytes, bob_bytes, edge_bytes) =
            (to_bytes(&alice), to_bytes(&bob), to_bytes(&edge));
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        writer
            .create_node_with_labels(alice, &[TypeId(3), TypeId(7)])
            .unwrap();
        writer.create_node(bob, TypeId(3)).unwrap();
        writer
            .set_properties(
                &alice,
                Some("Person"),
                HashMap::from([
                    ("name".into(), IrLiteral::Str("Alice".into())),
                    ("age".into(), IrLiteral::Int(42)),
                ]),
            )
            .unwrap();

        assert_eq!(
            writer.pending_node_labels(&HashSet::from([alice_bytes, bob_bytes])),
            HashSet::from([3, 7])
        );
        let matched = writer
            .find_pending_node(&[3, 7], &[("name".into(), IrLiteral::Str("Alice".into()))])
            .unwrap();
        assert_eq!(matched.0, alice_bytes);
        assert_eq!(matched.2, 3);
        assert_eq!(matched.3, vec![3, 7]);
        assert_eq!(matched.4["age"], IrLiteral::Int(42));
        assert!(writer.find_pending_node(&[9], &[]).is_none());
        assert!(
            writer
                .find_pending_node(&[3], &[("name".into(), IrLiteral::Str("Bob".into()))])
                .is_none()
        );

        assert_eq!(writer.add_pending_node_labels(&alice_bytes, &[7, 9]), 1);
        assert_eq!(writer.add_pending_node_labels(&[0xff; 16], &[1]), 0);
        assert_eq!(writer.remove_pending_node_labels(&alice_bytes, &[7, 99]), 1);
        assert_eq!(writer.remove_pending_node_labels(&[0xff; 16], &[1]), 0);
        assert_eq!(
            writer.pending_node_labels(&HashSet::from([alice_bytes])),
            HashSet::from([3, 9])
        );

        writer.create_edge(edge, "KNOWS", &alice, &bob).unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some("KNOWS"),
                HashMap::from([("since".into(), IrLiteral::Int(2024))]),
            )
            .unwrap();
        let direct = writer
            .find_pending_edge(
                "KNOWS",
                &alice_bytes,
                &bob_bytes,
                false,
                &[("since".into(), IrLiteral::Int(2024))],
            )
            .unwrap();
        assert_eq!(direct.0, edge_bytes);
        assert_eq!(direct.1, alice_bytes);
        assert_eq!(direct.2, bob_bytes);
        assert_eq!(direct.3["since"], IrLiteral::Int(2024));
        assert!(
            writer
                .find_pending_edge("KNOWS", &bob_bytes, &alice_bytes, false, &[])
                .is_none()
        );
        assert!(
            writer
                .find_pending_edge("KNOWS", &bob_bytes, &alice_bytes, true, &[])
                .is_some()
        );
        assert!(
            writer
                .find_pending_edge("IGNORES", &alice_bytes, &bob_bytes, false, &[])
                .is_none()
        );

        writer.flush().unwrap();
        let mut reopened = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS + 1).unwrap();
        assert_eq!(reopened.create_node(new_v7(), TypeId(3)).unwrap(), 3);
        assert_eq!(
            read_node_props(dir.path(), "Person")[&alice_bytes]["name"],
            IrLiteral::Str("Alice".into())
        );
        assert_eq!(
            read_edge_props(dir.path(), "KNOWS")[&edge_bytes]["since"],
            IrLiteral::Int(2024)
        );
    }

    #[test]
    fn merge_and_remove_pending_props_edit_buffered_rows() {
        let dir = TempDir::new().unwrap();
        let a = new_v7();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([
                ("name".to_owned(), IrLiteral::Str("old".into())),
                ("age".to_owned(), IrLiteral::Int(30)),
            ]),
        )
        .unwrap();

        // SET on the pending node: overwrite one key, add another.
        w.merge_pending_node_props(
            &to_bytes(&a),
            None,
            HashMap::from([
                ("name".to_owned(), IrLiteral::Str("new".into())),
                ("city".to_owned(), IrLiteral::Str("Oslo".into())),
            ]),
        );
        // REMOVE on the pending node: drop a key; absent keys are no-ops.
        w.remove_pending_node_props(
            &to_bytes(&a),
            &HashSet::from(["age".to_owned(), "absent".to_owned()]),
        );

        w.flush().unwrap();
        let props = &read_node_props(dir.path(), "_untyped")[&to_bytes(&a)];
        assert_eq!(props["name"], IrLiteral::Str("new".into()));
        assert_eq!(props["city"], IrLiteral::Str("Oslo".into()));
        assert!(!props.contains_key("age"), "removed before flush");
    }

    #[test]
    fn flush_into_composes_with_staged_delete_in_one_batch() {
        // The #792 statement shape: DELETE a committed node and CREATE a new
        // one in the same statement — one RewriteBatch, one commit, with the
        // flush reading through the delete's staged nodes.parquet content.
        let dir = TempDir::new().unwrap();
        let (a, b) = (new_v7(), new_v7());
        let mut seed = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        seed.create_node(a, TypeId(0)).unwrap();
        seed.create_node(b, TypeId(0)).unwrap();
        seed.set_properties(
            &a,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
        )
        .unwrap();
        seed.flush().unwrap();

        let mut staged = RewriteBatch::new();
        let removed = crate::mutator::stage_delete_nodes(
            &mut staged,
            dir.path(),
            &HashSet::from([to_bytes(&a)]),
        )
        .unwrap();
        assert_eq!(removed, 1);

        let d = new_v7();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(d, TypeId(0)).unwrap();
        w.flush_into(&mut staged).unwrap();

        // nodes.parquet staged exactly once (net content), still last-ish in
        // commit order relative to the delete's property rewrite.
        let staged_nodes = staged
            .staged_paths()
            .filter(|p| p.ends_with("topology/nodes.parquet"))
            .count();
        assert_eq!(staged_nodes, 1, "net content, no double-stage");

        // Nothing visible before commit.
        let pre: usize = crate::catalog::read_nodes(dir.path())
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(pre, 2);

        staged.commit().unwrap();
        let nodes = crate::catalog::read_nodes(dir.path()).unwrap();
        let total: usize = nodes.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2, "b survives, a deleted, d created");
        assert!(
            !read_node_props(dir.path(), "_untyped").contains_key(&to_bytes(&a)),
            "deleted node's props gone"
        );
    }

    #[test]
    fn every_persisted_property_family_round_trips_through_parquet_reopen() {
        let dir = TempDir::new().unwrap();
        let node = new_v7();
        let propertyless = new_v7();
        let values = HashMap::from([
            ("int".into(), IrLiteral::Int(-7)),
            ("float".into(), IrLiteral::Float(2.5)),
            ("bool".into(), IrLiteral::Bool(true)),
            ("str".into(), IrLiteral::Str("value".into())),
            (
                "duration".into(),
                IrLiteral::Duration {
                    months: 1,
                    days: -2,
                    seconds: 3,
                    nanos: 4,
                },
            ),
            ("datetime".into(), IrLiteral::DateTime(TS)),
            ("date".into(), IrLiteral::Date(19_000)),
            (
                "local_datetime".into(),
                IrLiteral::LocalDateTime {
                    days: 19_001,
                    nanos: 123,
                },
            ),
            ("time".into(), IrLiteral::Time(456)),
            (
                "zoned_time".into(),
                IrLiteral::ZonedTime {
                    nanos: 789,
                    offset: -21_600,
                },
            ),
            (
                "zoned_datetime".into(),
                IrLiteral::ZonedDateTime {
                    days: 19_002,
                    nanos: 987,
                    offset: 3_600,
                    zone: Some("Europe/Paris".into()),
                },
            ),
            (
                "offset_datetime".into(),
                IrLiteral::ZonedDateTime {
                    days: 19_003,
                    nanos: 654,
                    offset: 0,
                    zone: None,
                },
            ),
            (
                "ints".into(),
                IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Null, IrLiteral::Int(3)]),
            ),
            (
                "dates".into(),
                IrLiteral::List(vec![IrLiteral::Date(19_004), IrLiteral::Date(19_005)]),
            ),
            ("empty".into(), IrLiteral::List(Vec::new())),
            ("null".into(), IrLiteral::Null),
        ]);
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        writer.create_node(node, TypeId(0)).unwrap();
        writer.create_node(propertyless, TypeId(0)).unwrap();
        writer.set_properties(&node, None, values.clone()).unwrap();
        writer
            .set_properties(
                &propertyless,
                None,
                values
                    .keys()
                    .map(|name| (name.clone(), IrLiteral::Null))
                    .collect(),
            )
            .unwrap();
        writer.flush().unwrap();

        let reopened = read_node_props(dir.path(), "_untyped");
        let actual = reopened.get(&to_bytes(&node)).unwrap();
        for (name, expected) in &values {
            if matches!(expected, IrLiteral::Null) {
                assert!(!actual.contains_key(name));
            } else if name == "empty" {
                assert_eq!(actual.get(name), Some(&IrLiteral::Str("[]".into())));
            } else {
                assert_eq!(actual.get(name), Some(expected), "property {name}");
            }
        }
        assert!(
            reopened
                .get(&to_bytes(&propertyless))
                .is_none_or(HashMap::is_empty)
        );
    }

    #[test]
    fn canonical_spatial_properties_round_trip_with_exact_geoarrow_metadata() {
        let dir = TempDir::new().unwrap();
        let node = new_v7();
        let other = new_v7();
        let edge = new_v7();
        let spatial = |geometry, coordinates| {
            IrLiteral::Spatial(SpatialValue {
                spatial_type: SpatialType {
                    geometry,
                    crs: SpatialCrs::Epsg4326,
                },
                coordinates,
                extension_name: None,
                extension_metadata: None,
            })
        };
        let values = HashMap::from([
            (
                "point".into(),
                spatial(
                    SpatialGeometryType::Point,
                    SpatialCoordinates::Point([-104.9903, 39.7392]),
                ),
            ),
            (
                "line".into(),
                spatial(
                    SpatialGeometryType::LineString,
                    SpatialCoordinates::LineString(vec![[0.0, 1.0], [2.0, 3.0]]),
                ),
            ),
            (
                "polygon".into(),
                spatial(
                    SpatialGeometryType::Polygon,
                    SpatialCoordinates::Polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]]),
                ),
            ),
            (
                "multipoint".into(),
                spatial(
                    SpatialGeometryType::MultiPoint,
                    SpatialCoordinates::MultiPoint(vec![[4.0, 5.0], [6.0, 7.0]]),
                ),
            ),
            (
                "multiline".into(),
                spatial(
                    SpatialGeometryType::MultiLineString,
                    SpatialCoordinates::MultiLineString(vec![vec![[8.0, 9.0], [10.0, 11.0]]]),
                ),
            ),
            (
                "multipolygon".into(),
                spatial(
                    SpatialGeometryType::MultiPolygon,
                    SpatialCoordinates::MultiPolygon(vec![vec![vec![
                        [0.0, 0.0],
                        [2.0, 0.0],
                        [0.0, 0.0],
                    ]]]),
                ),
            ),
            (
                "preserved".into(),
                IrLiteral::Spatial(SpatialValue {
                    spatial_type: SpatialType {
                        geometry: SpatialGeometryType::Point,
                        crs: SpatialCrs::Preserved("OGC:CRS84".into()),
                    },
                    coordinates: SpatialCoordinates::Point([-104.9903, 39.7392]),
                    extension_name: Some("geoarrow.vendor_point".into()),
                    extension_metadata: Some(
                        "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}"
                            .into(),
                    ),
                }),
            ),
        ]);

        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        writer.create_node(node, TypeId(0)).unwrap();
        writer.create_node(other, TypeId(0)).unwrap();
        writer.create_edge(edge, "ROUTE", &node, &other).unwrap();
        writer.set_properties(&node, None, values.clone()).unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some("ROUTE"),
                HashMap::from([("location".into(), values["point"].clone())]),
            )
            .unwrap();
        writer.flush().unwrap();

        let node_schema =
            crate::catalog::discover_parquet_schema(&node_props_path(dir.path(), "_untyped"))
                .unwrap();
        for (name, extension_name) in [
            ("point", "geoarrow.point"),
            ("line", "geoarrow.linestring"),
            ("polygon", "geoarrow.polygon"),
            ("multipoint", "geoarrow.multipoint"),
            ("multiline", "geoarrow.multilinestring"),
            ("multipolygon", "geoarrow.multipolygon"),
        ] {
            let field = node_schema.field_with_name(name).unwrap();
            assert_eq!(field.metadata()["ARROW:extension:name"], extension_name);
            assert_eq!(
                field.metadata()["ARROW:extension:metadata"],
                "{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\"}"
            );
        }
        let preserved = node_schema.field_with_name("preserved").unwrap();
        assert_eq!(
            preserved.metadata()["ARROW:extension:name"],
            "geoarrow.vendor_point"
        );
        assert_eq!(
            preserved.metadata()["ARROW:extension:metadata"],
            "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}"
        );
        assert_eq!(
            read_node_props(dir.path(), "_untyped")[&to_bytes(&node)],
            values
        );
        assert_eq!(
            read_edge_props(dir.path(), "ROUTE")[&to_bytes(&edge)]["location"],
            values["point"]
        );

        // Opening and flushing again exercises the persisted decode/re-encode path.
        let mut reopened =
            GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 1).unwrap();
        reopened.flush().unwrap();
        assert_eq!(
            read_node_props(dir.path(), "_untyped")[&to_bytes(&node)],
            values
        );
    }

    #[test]
    fn property_literal_rendering_and_nested_invalid_values_are_deterministic() {
        let uuid = [0xabu8; 16];
        let cases = [
            (IrLiteral::Null, "".into()),
            (IrLiteral::Bool(true), "true".into()),
            (IrLiteral::Int(-2), "-2".into()),
            (IrLiteral::Float(1.25), "1.25".into()),
            (IrLiteral::Str("s".into()), "s".into()),
            (IrLiteral::Uuid(uuid), "ab".repeat(16)),
            (
                IrLiteral::Duration {
                    months: 1,
                    days: 2,
                    seconds: 3,
                    nanos: 4,
                },
                "1mo2d3s4ns".into(),
            ),
            (IrLiteral::DateTime(5), "5".into()),
            (IrLiteral::Date(6), "6".into()),
            (
                IrLiteral::LocalDateTime { days: 7, nanos: 8 },
                "7d8ns".into(),
            ),
            (IrLiteral::Time(9), "9ns".into()),
            (
                IrLiteral::ZonedTime {
                    nanos: 10,
                    offset: -1,
                },
                "10ns-1s".into(),
            ),
            (
                IrLiteral::ZonedDateTime {
                    days: 11,
                    nanos: 12,
                    offset: 13,
                    zone: Some("UTC".into()),
                },
                "11d12ns+13sUTC".into(),
            ),
            (
                IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Str("x".into())]),
                "[1,x]".into(),
            ),
            (
                IrLiteral::Map(vec![("a".into(), IrLiteral::Bool(false))]),
                "{a:false}".into(),
            ),
        ];
        for (literal, expected) in cases {
            assert_eq!(literal_to_string(&literal), expected);
        }

        for invalid in [
            IrLiteral::Uuid(uuid),
            IrLiteral::List(vec![IrLiteral::Uuid(uuid)]),
            IrLiteral::Map(vec![("nested".into(), IrLiteral::Uuid(uuid))]),
        ] {
            assert_eq!(
                reject_map_property_value("p", &invalid).unwrap_err().code(),
                "GF_VALIDATION"
            );
        }
        for invalid in [
            IrLiteral::Map(vec![]),
            IrLiteral::List(vec![IrLiteral::Map(vec![])]),
        ] {
            assert_eq!(
                reject_map_property_value("p", &invalid).unwrap_err().code(),
                "GF_IO"
            );
        }
    }

    #[test]
    fn persisted_property_decoder_rejects_unsupported_shape_type_and_dynamic_array() {
        use arrow::array::{Int32Array, Int64Array, StructArray, UInt8Array};
        use arrow::datatypes::{DataType, Field, Fields};

        let unsupported: arrow::array::ArrayRef = Arc::new(UInt8Array::from(vec![1]));
        let unsupported_field = Field::new("unsupported", DataType::UInt8, false);
        assert!(decode_value(&unsupported, &unsupported_field, 0).is_err());

        let fields: Fields = vec![Field::new("other", DataType::Int32, false)].into();
        let structure: arrow::array::ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        ));
        let structure_field = Field::new("structure", DataType::Struct(fields), false);
        assert!(decode_value(&structure, &structure_field, 0).is_err());

        let wrong_dynamic: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let declared = Field::new("declared", DataType::UInt64, false);
        assert!(decode_value(&wrong_dynamic, &declared, 0).is_err());
    }
}
