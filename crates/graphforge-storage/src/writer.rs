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
//! 1. [`flush`](GraphWriter::flush) stages one immutable bounded fragment per
//!    non-empty node, relation, and property route. Prior construction fragments
//!    are never decoded or rewritten. Each statement still uses one ordered
//!    [`RewriteBatch`] commit boundary (#790).
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, Float64Array,
    Float64Builder, Int64Builder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use graphforge_core::uuid::{Uuid, to_bytes};
use graphforge_core::{
    GfError, OntologyMode, ProjectErrorCode, SpatialCoordinates, SpatialCrs, SpatialGeometryType,
    SpatialType, SpatialValue,
};
use graphforge_ir::IrLiteral;

/// Identity, labels, and properties of a buffered node matched by MERGE.
pub type PendingNodeMatch = (
    [u8; 16],
    u64,
    PrimaryEntityTypeId,
    Vec<EntityTypeId>,
    HashMap<String, IrLiteral>,
);

use graphforge_value::{EntityTypeId, PrimaryEntityTypeId};

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
const SURROGATE_TAILS_FILE: &str = "topology/surrogate_tails.parquet";

fn surrogate_tails_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("max_node_id", DataType::UInt64, false),
        Field::new("max_edge_id", DataType::UInt64, false),
    ]))
}

pub(crate) fn read_surrogate_tails(dir: &Path) -> Result<Option<(u64, u64)>, GfError> {
    let path = dir.join(SURROGATE_TAILS_FILE);
    let input = match fs::File::open(&path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_err(&error)),
    };
    read_surrogate_tails_file(input).map(Some)
}

pub(crate) fn read_surrogate_tails_file<R>(input: R) -> Result<(u64, u64), GfError>
where
    R: parquet::file::reader::ChunkReader + 'static,
{
    use arrow::array::Array;

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(input)
        .map_err(pq_err)?
        .with_batch_size(2)
        .build()
        .map_err(pq_err)?;
    let batch = reader
        .next()
        .ok_or_else(|| GfError::Storage("surrogate tails contain no row".into()))?
        .map_err(pq_err)?;
    if batch.num_rows() != 1 || reader.next().is_some() {
        return Err(GfError::Storage(
            "surrogate tails must contain exactly one row".into(),
        ));
    }
    let value = |name: &str| -> Result<u64, GfError> {
        let column = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| GfError::Storage(format!("surrogate tails lack {name}")))?;
        if column.is_null(0) {
            return Err(GfError::Storage(format!("surrogate tails {name} is null")));
        }
        Ok(column.value(0))
    };
    Ok((value("max_node_id")?, value("max_edge_id")?))
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn io_err(e: &std::io::Error) -> GfError {
    GfError::Storage(e.to_string())
}

fn pq_err(e: impl fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

fn replay_resource_limit(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}

fn replay_writer_properties(max_batch_rows: usize) -> parquet::file::properties::WriterProperties {
    crate::permanent_parquet::writer_properties()
        .set_max_row_group_row_count(Some(max_batch_rows))
        .set_dictionary_enabled(false)
        .build()
}

const REPLAY_NODE_FIXED_ROW_BYTES: usize = 128;

fn parquet_reader_metadata_reservation(path: &Path) -> Result<usize, GfError> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = fs::File::open(path).map_err(|error| io_err(&error))?;
    let length = file.metadata().map_err(|error| io_err(&error))?.len();
    if length < 8 {
        return Err(pq_err("canonical Parquet footer is truncated"));
    }
    file.seek(SeekFrom::End(-8))
        .map_err(|error| io_err(&error))?;
    let mut footer = [0_u8; 8];
    file.read_exact(&mut footer)
        .map_err(|error| io_err(&error))?;
    if &footer[4..] != b"PAR1" {
        return Err(pq_err("canonical Parquet footer magic is invalid"));
    }
    let encoded = usize::try_from(u32::from_le_bytes(footer[..4].try_into().unwrap()))
        .map_err(|_| replay_resource_limit("Parquet metadata length overflows memory"))?;
    encoded
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(|| replay_resource_limit("Parquet reader metadata reservation overflow"))
}

fn replay_reader_reservation(
    path: &Path,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
) -> Result<usize, GfError> {
    let footer = parquet_reader_metadata_reservation(path)?;
    if footer > limits.max_replay_memory_bytes {
        return Err(replay_resource_limit(
            "Parquet reader footer exceeds replay budget",
        ));
    }
    let file = fs::File::open(path).map_err(|error| io_err(&error))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        file.try_clone().map_err(|error| io_err(&error))?,
    )
    .map_err(pq_err)?;
    let metadata_bytes = builder.metadata().memory_size();
    let available = limits
        .max_replay_memory_bytes
        .checked_sub(metadata_bytes)
        .ok_or_else(|| replay_resource_limit("Parquet reader metadata exceeds replay budget"))?;
    let decoder = crate::property_overlay::replay_parquet_reader_reservation(
        builder.metadata(),
        &file,
        available,
        limits.max_batch_rows,
    )?;
    metadata_bytes
        .checked_add(decoder)
        .map(|bytes| bytes.max(footer))
        .ok_or_else(|| replay_resource_limit("Parquet reader reservation overflow"))
}

fn replay_writer_reservation(
    schema: &Schema,
    maximum_rows: usize,
    maximum_row_bytes: usize,
    max_batch_rows: usize,
) -> Result<usize, GfError> {
    let groups = maximum_rows.div_ceil(max_batch_rows);
    let structure_bytes = crate::permanent_parquet::replay_writer_structure_bytes(schema)?;
    let metadata_bytes = crate::permanent_parquet::replay_metadata_bytes(
        schema,
        groups,
        max_batch_rows.min(maximum_rows),
        maximum_row_bytes,
    )?;
    let active_rows = max_batch_rows.min(maximum_rows);
    let active_buffer_bytes = maximum_row_bytes
        .checked_mul(active_rows)
        .ok_or_else(|| replay_resource_limit("graph delta replay active buffer overflow"))?;
    let encoder_bytes = if maximum_row_bytes == 0 {
        0 // Property chunks reserve their actual aggregate snapshot bytes below.
    } else {
        crate::permanent_parquet::replay_encoder_buffers(schema, active_buffer_bytes, active_rows)?
    };
    structure_bytes
        .checked_add(encoder_bytes)
        .and_then(|bytes| bytes.checked_add(metadata_bytes))
        .and_then(|bytes| bytes.checked_add(active_buffer_bytes))
        .ok_or_else(|| replay_resource_limit("graph delta replay writer reservation overflow"))
}

fn admit_replay_writer(
    overlay_bytes: usize,
    authority_bytes: usize,
    reservation_bytes: usize,
    limit: usize,
    context: &str,
) -> Result<(), GfError> {
    if overlay_bytes
        .checked_add(authority_bytes)
        .and_then(|bytes| bytes.checked_add(reservation_bytes))
        .is_none_or(|bytes| bytes > limit)
    {
        return Err(replay_resource_limit(format!(
            "graph delta replay {context} writer memory bound exceeded: overlay={overlay_bytes} authority={authority_bytes} writer={reservation_bytes} limit={limit}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Buffered rows
// ---------------------------------------------------------------------------

struct NodeRow {
    node_uuid: [u8; 16],
    node_id: u64,
    type_id: PrimaryEntityTypeId,
    type_ids: Vec<EntityTypeId>,
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

type PropertyGenerationAuthority = Option<(uuid::Uuid, PathBuf)>;
type CompletedPropertyWindow<R> = (Vec<R>, Option<SchemaRef>, PropertyGenerationAuthority);

type TypedPropertyRow = (String, [u8; 16], HashMap<String, IrLiteral>);
#[cfg(test)]
type ReconstructedEdge<'a> = (&'a String, &'a (String, String, String));

/// Apply a bounded GFDR overlay while scanning the canonical base in bounded
/// Arrow batches. Only overlay identities are retained across batches.
pub(crate) fn write_replay_overlay_streaming(
    source: &Path,
    source_inventory: &crate::GraphFilesInventory,
    target: &Path,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
) -> Result<ReplayNodeSpoolEvidence, GfError> {
    if !overlay.nodes.is_empty() || !overlay.edges.is_empty() {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::UnsupportedProjectFormat,
            message:
                "GFDR supports property mutations only; topology requires canonical publication"
                    .into(),
        });
    }
    let mut target_routes = crate::route_component::owned::admit_owned_workspace(target)?;
    let property_inventory = crate::AuthenticatedPropertyInventory::from_inventory_at_root(
        source,
        source_inventory.clone(),
        None,
    )?;
    let node_scan = stream_replay_nodes(source, target, overlay, limits)?;

    validate_replay_edge_endpoints(overlay, &node_scan)?;
    stream_replay_edges(
        target,
        &property_inventory,
        &mut target_routes,
        overlay,
        limits,
        &node_scan,
    )?;
    stream_replay_properties(
        target,
        &property_inventory,
        overlay,
        limits,
        false,
        &mut target_routes,
    )?;
    stream_replay_properties(
        target,
        &property_inventory,
        overlay,
        limits,
        true,
        &mut target_routes,
    )?;
    let node_properties_changed =
        !overlay.node_properties.is_empty() || overlay.nodes.values().any(Option::is_none);
    let properties_changed = node_properties_changed
        || !overlay.edge_properties.is_empty()
        || overlay.edges.values().any(Option::is_none);
    replace_private_replay_route_table(
        target,
        &target_routes,
        properties_changed,
        node_properties_changed,
    )?;
    Ok(node_scan.spool_evidence)
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
            .map(|(uuid, _)| {
                state
                    .node_primary_types
                    .get(*uuid)
                    .map(|primary| primary.encode())
                    .ok_or_else(|| {
                        pq_err(format!(
                            "reconstructed node {uuid} is missing its primary route"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let nullable_label_sets =
        arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
            nodes
                .iter()
                .map(|(_, labels)| Some(labels.iter().map(|id| Some(id.encode())))),
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

/// Exact topology construction work performed by one writer session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopologyWriteWork {
    /// Topology rows accepted as construction input.
    pub input_rows: u64,
    /// Prior topology rows decoded while accepting this input.
    pub prior_rows_decoded: u64,
    /// Topology rows encoded into new immutable shards.
    pub rows_encoded: u64,
    /// Immutable topology shards produced.
    pub shard_count: u64,
    /// Physical bytes staged for immutable topology shards.
    pub output_bytes: u64,
    /// Prior topology rows decoded and re-encoded.
    pub existing_rows_rewritten: u64,
    /// Newly accepted topology rows encoded.
    pub new_rows_written: u64,
    /// Maximum topology rows retained between explicit batch releases.
    pub peak_buffered_rows: u64,
    /// Conservative peak bytes charged to topology construction state.
    pub peak_buffered_bytes: u64,
    /// Conservative peak scratch bytes required while encoding a flush.
    pub peak_flush_scratch_bytes: u64,
    /// Authenticated UUID-index block-positioning seeks for endpoint lookup.
    pub uuid_block_seeks: u64,
    /// Authenticated identity blocks read for endpoint lookup.
    pub uuid_identity_blocks_read: u64,
    /// Authenticated identity bytes read for endpoint lookup.
    pub uuid_identity_bytes_read: u64,
    /// Authenticated reverse-surrogate blocks read for pair validation.
    pub uuid_surrogate_blocks_read: u64,
    /// Authenticated reverse-surrogate bytes read for pair validation.
    pub uuid_surrogate_bytes_read: u64,
    /// Immutable UUID runs considered with newest-run shadowing.
    pub uuid_runs_considered: u64,
    /// Per-record filesystem seeks. This remains zero for batched lookup.
    pub uuid_per_record_seeks: u64,
    /// UUID identity/tombstone records accepted by committed index deltas.
    pub uuid_input_records: u64,
    /// Prior topology rows decoded by UUID index publication; ordinary append is zero.
    pub uuid_prior_topology_rows_decoded: u64,
    /// Physical UUID run and manifest bytes written by committed deltas.
    pub uuid_physical_bytes_written: u64,
    /// UUID output blocks submitted by committed deltas.
    pub uuid_write_blocks: u64,
    /// UUID output bytes submitted by committed deltas.
    pub uuid_write_bytes: u64,
    /// Peak fixed-width UUID records buffered by a committed delta.
    pub uuid_peak_buffered_records: u64,
    /// Peak charged fixed-width UUID bytes buffered by a committed delta.
    pub uuid_peak_buffered_bytes: u64,
    /// Retained UUID validation blocks read by committed deltas.
    pub uuid_validation_blocks: u64,
    /// Retained UUID validation bytes read by committed deltas.
    pub uuid_validation_bytes: u64,
    /// Per-record random seeks during UUID publication validation; always zero.
    pub uuid_validation_random_seeks: u64,
}

/// Explicit capability limits for graph construction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphWriterLimits {
    /// Maximum buffered node, edge, and property rows.
    pub max_buffered_topology_rows: usize,
    /// Maximum conservative charged bytes for retained topology state.
    pub max_buffered_topology_bytes: usize,
    /// Maximum conservative temporary Arrow/Parquet input bytes per flush.
    pub max_flush_scratch_bytes: usize,
}

impl Default for GraphWriterLimits {
    fn default() -> Self {
        Self {
            max_buffered_topology_rows: 65_536,
            max_buffered_topology_bytes: 64 * 1024 * 1024,
            max_flush_scratch_bytes: 64 * 1024 * 1024,
        }
    }
}

const NODE_ROW_CHARGE: usize = 256;
const EDGE_ROW_CHARGE: usize = 384;
const ENDPOINT_ENTRY_CHARGE: usize = 128;
const ROUTE_ENTRY_CHARGE: usize = 128;
const NODE_SCRATCH_CHARGE: usize = 96;
const EDGE_SCRATCH_CHARGE: usize = 160;
const PROPERTY_ROW_CHARGE: usize = 128;
const PROPERTY_ENTRY_CHARGE: usize = 96;

fn literal_dynamic_bytes(value: &IrLiteral) -> usize {
    match value {
        IrLiteral::Str(value) => value.len(),
        IrLiteral::ZonedDateTime { zone, .. } => zone.as_ref().map_or(0, String::len),
        IrLiteral::List(values) => values.iter().fold(0usize, |sum, value| {
            sum.saturating_add(size_of::<IrLiteral>())
                .saturating_add(literal_dynamic_bytes(value))
        }),
        IrLiteral::Map(entries) => entries.iter().fold(0usize, |sum, (key, value)| {
            sum.saturating_add(size_of::<(String, IrLiteral)>())
                .saturating_add(key.len())
                .saturating_add(literal_dynamic_bytes(value))
        }),
        _ => 0,
    }
}

fn property_map_charge(props: &HashMap<String, IrLiteral>) -> usize {
    props.iter().fold(0usize, |sum, (key, value)| {
        sum.saturating_add(PROPERTY_ENTRY_CHARGE)
            .saturating_add(key.len())
            .saturating_add(literal_dynamic_bytes(value))
    })
}

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
    pending_index_nodes: Vec<(Uuid, u64)>,
    pending_index_edges: Vec<Uuid>,
    uuid_index_snapshot: Option<crate::AuthenticatedUuidIndexSnapshot>,
    uuid_snapshot_refresh_needed: Option<u64>,
    limits: GraphWriterLimits,
    charged_topology_bytes: usize,
    buffered_topology_rows: usize,
    flush_scratch_bytes: usize,
    semantic_composition_fingerprint: Option<String>,
    topology_work: TopologyWriteWork,
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
        crate::route_component::owned::admit_owned_workspace(dir)?;
        // Continue surrogate assignment from the on-disk maximum so a writer
        // opened on an existing project appends rather than colliding with /
        // overwriting prior rows. Absent files → max 0 → start at 1.
        let (max_node_id, max_edge_id) = match read_surrogate_tails(dir)? {
            Some(tails) => tails,
            None => (
                crate::catalog::max_node_id(dir).map_err(pq_err)?,
                crate::catalog::max_edge_id(dir).map_err(pq_err)?,
            ),
        };
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
            pending_index_nodes: Vec::new(),
            pending_index_edges: Vec::new(),
            uuid_index_snapshot: None,
            uuid_snapshot_refresh_needed: None,
            limits: GraphWriterLimits::default(),
            charged_topology_bytes: 0,
            buffered_topology_rows: 0,
            flush_scratch_bytes: 0,
            semantic_composition_fingerprint: None,
            topology_work: TopologyWriteWork::default(),
        })
    }

    /// Exact bounded topology construction work accumulated by this writer.
    #[must_use]
    pub fn topology_write_work(&self) -> TopologyWriteWork {
        self.topology_work
    }

    /// Configure the explicit topology construction capability.
    #[must_use]
    pub fn with_limits(mut self, limits: GraphWriterLimits) -> Self {
        self.limits = limits;
        self
    }

    fn admit_topology(&mut self, rows: usize, bytes: usize, scratch: usize) -> Result<(), GfError> {
        let next_rows = self
            .buffered_topology_rows
            .checked_add(rows)
            .ok_or_else(|| GfError::Storage("graph writer topology row charge overflow".into()))?;
        let next_bytes = self
            .charged_topology_bytes
            .checked_add(bytes)
            .ok_or_else(|| GfError::Storage("graph writer topology byte charge overflow".into()))?;
        let next_scratch = self
            .flush_scratch_bytes
            .checked_add(scratch)
            .ok_or_else(|| GfError::Storage("graph writer scratch charge overflow".into()))?;
        if next_rows > self.limits.max_buffered_topology_rows
            || next_bytes > self.limits.max_buffered_topology_bytes
            || next_scratch > self.limits.max_flush_scratch_bytes
        {
            return Err(GfError::Storage(
                "graph writer topology construction window exhausted".into(),
            ));
        }
        self.buffered_topology_rows = next_rows;
        self.charged_topology_bytes = next_bytes;
        self.flush_scratch_bytes = next_scratch;
        self.topology_work.peak_buffered_rows =
            self.topology_work.peak_buffered_rows.max(next_rows as u64);
        self.topology_work.peak_buffered_bytes = self
            .topology_work
            .peak_buffered_bytes
            .max(next_bytes as u64);
        self.topology_work.peak_flush_scratch_bytes = self
            .topology_work
            .peak_flush_scratch_bytes
            .max(next_scratch as u64);
        Ok(())
    }

    fn refresh_topology_charge(&mut self) {
        let node_bytes = self.nodes.iter().fold(0usize, |sum, row| {
            sum.saturating_add(NODE_ROW_CHARGE)
                .saturating_add(size_of::<(Uuid, u64)>())
                .saturating_add(row.type_ids.len().saturating_mul(size_of::<EntityTypeId>()))
        });
        let edge_bytes = self.edges.iter().fold(0usize, |sum, (route, rows)| {
            sum.saturating_add(ROUTE_ENTRY_CHARGE)
                .saturating_add(route.len().saturating_mul(2))
                .saturating_add(rows.iter().fold(0usize, |rows_sum, row| {
                    rows_sum
                        .saturating_add(EDGE_ROW_CHARGE)
                        .saturating_add(size_of::<Uuid>())
                        .saturating_add(size_of::<crate::adjacency_delta::DeltaEdge>())
                        .saturating_add(
                            row.rel_type_name
                                .as_ref()
                                .map_or(0, |name| name.len().saturating_mul(2)),
                        )
                        .saturating_add(row.rel_type_name.as_ref().map_or(route.len(), String::len))
                }))
        });
        let endpoint_bytes = self
            .uuid_to_node_id
            .len()
            .saturating_mul(ENDPOINT_ENTRY_CHARGE);
        let delta_bytes = self.pending_delta.iter().fold(0usize, |sum, edge| {
            sum.saturating_add(size_of::<crate::adjacency_delta::DeltaEdge>())
                .saturating_add(edge.rel_type_name.len())
        });
        let property_bytes = self.properties.iter().fold(0usize, |sum, (route, rows)| {
            sum.saturating_add(ROUTE_ENTRY_CHARGE)
                .saturating_add(route.len())
                .saturating_add(rows.iter().fold(0usize, |rows_sum, row| {
                    rows_sum
                        .saturating_add(PROPERTY_ROW_CHARGE)
                        .saturating_add(property_map_charge(&row.props))
                }))
        });
        let edge_property_bytes = self
            .edge_properties
            .iter()
            .fold(0usize, |sum, (route, rows)| {
                sum.saturating_add(ROUTE_ENTRY_CHARGE)
                    .saturating_add(route.len())
                    .saturating_add(rows.iter().fold(0usize, |rows_sum, row| {
                        rows_sum
                            .saturating_add(PROPERTY_ROW_CHARGE)
                            .saturating_add(property_map_charge(&row.props))
                    }))
            });
        self.charged_topology_bytes = node_bytes
            .saturating_add(edge_bytes)
            .saturating_add(endpoint_bytes)
            .saturating_add(delta_bytes)
            .saturating_add(property_bytes)
            .saturating_add(edge_property_bytes);
        self.buffered_topology_rows = self
            .nodes
            .len()
            .saturating_add(self.edges.values().map(Vec::len).sum::<usize>())
            .saturating_add(self.properties.values().map(Vec::len).sum::<usize>())
            .saturating_add(self.edge_properties.values().map(Vec::len).sum::<usize>());
        self.flush_scratch_bytes = self
            .nodes
            .iter()
            .fold(0usize, |sum, row| {
                sum.saturating_add(NODE_SCRATCH_CHARGE)
                    .saturating_add(row.type_ids.len().saturating_mul(size_of::<EntityTypeId>()))
            })
            .saturating_add(self.edges.values().flatten().fold(0usize, |sum, row| {
                sum.saturating_add(EDGE_SCRATCH_CHARGE)
                    .saturating_add(row.rel_type_name.as_ref().map_or(0, String::len))
            }))
            .saturating_add(property_bytes)
            .saturating_add(edge_property_bytes);
    }

    fn admit_property_row(
        &mut self,
        stem: &str,
        props: &HashMap<String, IrLiteral>,
        edge: bool,
    ) -> Result<(), GfError> {
        let route_is_new = if edge {
            !self.edge_properties.contains_key(stem)
        } else {
            !self.properties.contains_key(stem)
        };
        let route =
            usize::from(route_is_new).saturating_mul(ROUTE_ENTRY_CHARGE.saturating_add(stem.len()));
        let retained = PROPERTY_ROW_CHARGE
            .saturating_add(property_map_charge(props))
            .saturating_add(route);
        self.admit_topology(1, retained, retained)
    }

    /// Release endpoint registrations after the caller has durably handed off
    /// the batch's UUID index and adjacency evidence.
    pub fn release_committed_topology_state(&mut self) {
        self.uuid_to_node_id.clear();
        self.pending_delta.clear();
        self.refresh_topology_charge();
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
    pub fn create_node(&mut self, node_uuid: Uuid, type_id: EntityTypeId) -> Result<u64, GfError> {
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
        type_ids: &[EntityTypeId],
    ) -> Result<u64, GfError> {
        let bytes = to_bytes(&node_uuid);
        if self.uuid_to_node_id.contains_key(&bytes)
            || self
                .edges
                .values()
                .flatten()
                .any(|row| row.edge_uuid == bytes)
        {
            return Err(GfError::Storage(
                "duplicate node UUID in graph writer topology window".into(),
            ));
        }
        let labels = type_ids.len().saturating_mul(size_of::<EntityTypeId>());
        self.admit_topology(
            1,
            NODE_ROW_CHARGE
                .saturating_add(ENDPOINT_ENTRY_CHARGE)
                // Reserve the immutable UUID-index duplicate at admission.
                .saturating_add(size_of::<(Uuid, u64)>())
                .saturating_add(labels),
            NODE_SCRATCH_CHARGE.saturating_add(labels),
        )?;
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        self.uuid_to_node_id.insert(bytes, node_id);
        self.nodes.push(NodeRow {
            node_uuid: bytes,
            node_id,
            type_id: type_ids
                .first()
                .copied()
                .map_or_else(PrimaryEntityTypeId::absent, PrimaryEntityTypeId::known),
            type_ids: type_ids.to_vec(),
        });
        self.pending_index_nodes.push((node_uuid, node_id));
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
    pub fn register_existing_node(&mut self, node_uuid: Uuid, node_id: u64) -> Result<(), GfError> {
        let key = to_bytes(&node_uuid);
        if let Some(existing) = self.uuid_to_node_id.get(&key) {
            return if *existing == node_id {
                Ok(())
            } else {
                Err(GfError::Storage(
                    "existing node UUID was registered with conflicting surrogates".into(),
                ))
            };
        }
        if self
            .edges
            .values()
            .flatten()
            .any(|row| row.edge_uuid == key)
        {
            return Err(GfError::Storage(
                "existing node UUID collides with a buffered edge UUID".into(),
            ));
        }
        self.admit_topology(0, ENDPOINT_ENTRY_CHARGE, 0)?;
        self.uuid_to_node_id.insert(key, node_id);
        Ok(())
    }

    /// Resolve and register persisted edge endpoints through the writer-owned
    /// authenticated disk-index snapshot. UUIDs are sorted/deduplicated and
    /// resolved with bounded block merge scans, so repeated construction
    /// batches decode zero topology rows and perform zero per-record seeks.
    pub fn register_existing_endpoints(
        &mut self,
        node_uuids: &[Uuid],
    ) -> Result<crate::UuidProbeMetrics, GfError> {
        if let Some(generation) = self.uuid_snapshot_refresh_needed {
            self.uuid_index_snapshot = Some(
                crate::AuthenticatedUuidIndexSnapshot::open_at_generation(&self.dir, generation)?,
            );
            self.uuid_snapshot_refresh_needed = None;
        }
        if self.uuid_index_snapshot.is_none() {
            crate::uuid_membership::ensure_uuid_membership_migrated(&self.dir)?;
            let generation = crate::read_topology_generation(&self.dir)?;
            self.uuid_index_snapshot = Some(
                crate::AuthenticatedUuidIndexSnapshot::open_at_generation(&self.dir, generation)?,
            );
        }
        let (surrogates, metrics) = self
            .uuid_index_snapshot
            .as_mut()
            .expect("snapshot initialized")
            .lookup_node_surrogates(node_uuids)?;
        let mut resolved = Vec::new();
        for (uuid, surrogate) in node_uuids.iter().zip(surrogates) {
            let surrogate = surrogate.ok_or_else(|| {
                GfError::Storage(format!(
                    "edge endpoint {} is absent from the authenticated node index",
                    graphforge_core::uuid::to_string(uuid)
                ))
            })?;
            let key = to_bytes(uuid);
            if !self.uuid_to_node_id.contains_key(&key)
                && !resolved.iter().any(|(candidate, _)| candidate == uuid)
            {
                resolved.push((*uuid, surrogate));
            }
        }
        self.admit_topology(0, resolved.len().saturating_mul(ENDPOINT_ENTRY_CHARGE), 0)?;
        for (uuid, surrogate) in resolved {
            self.uuid_to_node_id.insert(to_bytes(&uuid), surrogate);
        }
        self.topology_work.uuid_block_seeks = self
            .topology_work
            .uuid_block_seeks
            .saturating_add(metrics.file_seeks);
        self.topology_work.uuid_identity_blocks_read = self
            .topology_work
            .uuid_identity_blocks_read
            .saturating_add(metrics.identity_blocks_read);
        self.topology_work.uuid_identity_bytes_read = self
            .topology_work
            .uuid_identity_bytes_read
            .saturating_add(metrics.identity_bytes_read);
        self.topology_work.uuid_surrogate_blocks_read = self
            .topology_work
            .uuid_surrogate_blocks_read
            .saturating_add(metrics.surrogate_blocks_read);
        self.topology_work.uuid_surrogate_bytes_read = self
            .topology_work
            .uuid_surrogate_bytes_read
            .saturating_add(metrics.surrogate_bytes_read);
        self.topology_work.uuid_runs_considered = self
            .topology_work
            .uuid_runs_considered
            .saturating_add(metrics.runs_considered);
        self.topology_work.uuid_per_record_seeks = self
            .topology_work
            .uuid_per_record_seeks
            .saturating_add(metrics.per_record_seeks);
        Ok(metrics)
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

        let edge_bytes = to_bytes(&edge_uuid);
        if self.uuid_to_node_id.contains_key(&edge_bytes)
            || self
                .edges
                .values()
                .flatten()
                .any(|row| row.edge_uuid == edge_bytes)
        {
            return Err(GfError::Storage(
                "duplicate edge UUID in graph writer topology window".into(),
            ));
        }
        let route_is_new = match self.mode {
            OntologyMode::Exploratory => !self.edges.contains_key(EXPLORATORY_STEM),
            OntologyMode::Advisory | OntologyMode::Strict => !self.edges.contains_key(rel_type),
        };
        let route_charge = usize::from(route_is_new)
            .saturating_mul(ROUTE_ENTRY_CHARGE.saturating_add(rel_type.len().saturating_mul(2)));
        let dynamic = match self.mode {
            OntologyMode::Exploratory => rel_type.len().saturating_mul(2),
            OntologyMode::Advisory | OntologyMode::Strict => rel_type.len(),
        };
        self.admit_topology(
            1,
            EDGE_ROW_CHARGE
                .saturating_add(size_of::<Uuid>())
                .saturating_add(size_of::<crate::adjacency_delta::DeltaEdge>())
                .saturating_add(route_charge)
                .saturating_add(dynamic),
            EDGE_SCRATCH_CHARGE.saturating_add(rel_type.len()),
        )?;

        let edge_id = self.next_edge_id;
        self.next_edge_id += 1;

        let (stem, rel_type_name) = match self.mode {
            OntologyMode::Exploratory => (EXPLORATORY_STEM.to_owned(), Some(rel_type.to_owned())),
            OntologyMode::Advisory | OntologyMode::Strict => (rel_type.to_owned(), None),
        };

        self.edges.entry(stem).or_default().push(EdgeRow {
            edge_uuid: edge_bytes,
            src_uuid: src_bytes,
            dst_uuid: dst_bytes,
            edge_id,
            src_id,
            dst_id,
            rel_type_name,
        });
        self.pending_index_edges.push(edge_uuid);
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
        self.admit_property_row(&stem, &props, false)?;
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
        self.admit_property_row(&stem, &props, true)?;
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
    pub fn pending_node_labels(&self, targets: &HashSet<[u8; 16]>) -> HashSet<EntityTypeId> {
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
        let type_ids = UInt32Array::from(
            self.nodes
                .iter()
                .map(|r| r.type_id.encode())
                .collect::<Vec<_>>(),
        );
        let nullable_label_sets =
            arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
                self.nodes
                    .iter()
                    .map(|row| Some(row.type_ids.iter().map(|id| Some(id.encode())))),
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
        labels: &[EntityTypeId],
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
        labels: &[EntityTypeId],
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
        self.pending_index_nodes
            .retain(|(uuid, _)| !targets.contains(uuid.as_bytes()));
        self.refresh_topology_charge();
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
        self.pending_index_edges
            .retain(|uuid| !targets.contains(uuid.as_bytes()));
        self.refresh_topology_charge();
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
    ) -> Result<(), GfError> {
        let stem = match (self.mode, entity_type) {
            (OntologyMode::Advisory | OntologyMode::Strict, Some(t)) => t.to_owned(),
            _ => UNTYPED_STEM.to_owned(),
        };
        if let Some(current) = self
            .properties
            .get(&stem)
            .and_then(|rows| rows.iter().find(|r| &r.node_uuid == node_uuid))
        {
            let mut merged = current.props.clone();
            merged.extend(props);
            let retained =
                property_map_charge(&merged).saturating_sub(property_map_charge(&current.props));
            self.admit_topology(0, retained, retained)?;
            self.properties
                .get_mut(&stem)
                .expect("property stem remains present")
                .iter_mut()
                .find(|r| &r.node_uuid == node_uuid)
                .expect("property row remains present")
                .props = merged;
        } else {
            self.admit_property_row(&stem, &props, false)?;
            let rows = self.properties.entry(stem).or_default();
            rows.push(PropRow {
                node_uuid: *node_uuid,
                props,
            });
        }
        Ok(())
    }

    /// Add labels to a node buffered by this writer, preserving its primary label.
    pub fn add_pending_node_labels(
        &mut self,
        node_uuid: &[u8; 16],
        labels: &[EntityTypeId],
    ) -> u64 {
        let Some(row) = self
            .nodes
            .iter_mut()
            .find(|row| &row.node_uuid == node_uuid)
        else {
            return 0;
        };
        let before = row.type_ids.len();
        row.type_ids.extend(labels.iter().copied());
        row.type_ids.sort_unstable_by_key(|id| id.encode());
        row.type_ids.dedup();
        (row.type_ids.len() - before) as u64
    }

    /// Remove labels from a node buffered by this writer. The immutable scalar
    /// `type_id` remains only as the property-file routing key; `type_ids` is
    /// the authoritative membership set.
    pub fn remove_pending_node_labels(
        &mut self,
        node_uuid: &[u8; 16],
        labels: &[EntityTypeId],
    ) -> u64 {
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
    ) -> Result<(), GfError> {
        let stem = rel_type.unwrap_or(UNTYPED_STEM).to_owned();
        if let Some(current) = self
            .edge_properties
            .get(&stem)
            .and_then(|rows| rows.iter().find(|r| &r.edge_uuid == edge_uuid))
        {
            let mut merged = current.props.clone();
            merged.extend(props);
            let retained =
                property_map_charge(&merged).saturating_sub(property_map_charge(&current.props));
            self.admit_topology(0, retained, retained)?;
            self.edge_properties
                .get_mut(&stem)
                .expect("edge property stem remains present")
                .iter_mut()
                .find(|r| &r.edge_uuid == edge_uuid)
                .expect("edge property row remains present")
                .props = merged;
        } else {
            self.admit_property_row(&stem, &props, true)?;
            let rows = self.edge_properties.entry(stem).or_default();
            rows.push(EdgePropRow {
                edge_uuid: *edge_uuid,
                props,
            });
        }
        Ok(())
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

    /// Encode buffered rows into fresh immutable fragments, commit them as one
    /// ordered batch, then clear the row buffers.
    ///
    /// Only creates a subdirectory when there are rows to write into it. All
    /// files stage and commit as one batch (#790), nodes first: a failure while building any
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
        self.flush_into(&mut staged).map_err(|error| match error {
            GfError::Storage(message) => {
                GfError::Storage(format!("graph flush staging: {message}"))
            }
            other => other,
        })?;
        let pending = self.take_pending_delta();
        if let Some(generation) = self
            .commit_topology_aware_with_uuid_index(staged, Vec::new(), Vec::new())
            .map_err(|error| match error {
                GfError::Storage(message) => {
                    GfError::Storage(format!("graph flush commit: {message}"))
                }
                other => other,
            })?
        {
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
        self.refresh_topology_charge();
        edges
    }

    /// Commit topology and its UUID-index participant as one sealed durable rewrite.
    pub fn commit_topology_aware_with_uuid_index(
        &mut self,
        staged: RewriteBatch,
        deleted_nodes: Vec<Uuid>,
        deleted_edges: Vec<Uuid>,
    ) -> Result<Option<u64>, GfError> {
        if let Some(generation) = self.uuid_snapshot_refresh_needed {
            self.uuid_index_snapshot = Some(
                crate::AuthenticatedUuidIndexSnapshot::open_at_generation(&self.dir, generation)?,
            );
            self.uuid_snapshot_refresh_needed = None;
        }
        let committed = crate::uuid_membership::commit_uuid_topology_rewrite(
            &self.dir,
            staged,
            &crate::uuid_membership::UuidTopologyDelta {
                nodes: self.pending_index_nodes.clone(),
                edges: self.pending_index_edges.clone(),
                deleted_nodes,
                deleted_edges,
            },
            &mut self.uuid_index_snapshot,
        )?;
        match committed {
            crate::uuid_membership::CommittedUuidTopologyRewrite::NoTopologyChange => Ok(None),
            crate::uuid_membership::CommittedUuidTopologyRewrite::Committed {
                generation,
                metrics,
                v4_metrics,
            } => {
                self.record_uuid_append_work(&metrics);
                if let Some(metrics) = v4_metrics.as_ref() {
                    self.record_v4_ordinal_append_work(metrics);
                }
                self.pending_index_nodes.clear();
                self.pending_index_edges.clear();
                Ok(Some(generation))
            }
            crate::uuid_membership::CommittedUuidTopologyRewrite::CommittedNeedsRefresh {
                generation,
                metrics,
                v4_metrics,
                error,
            } => {
                self.record_uuid_append_work(&metrics);
                if let Some(metrics) = v4_metrics.as_ref() {
                    self.record_v4_ordinal_append_work(metrics);
                }
                self.pending_index_nodes.clear();
                self.pending_index_edges.clear();
                self.uuid_snapshot_refresh_needed = Some(generation);
                Err(GfError::Storage(format!(
                    "topology generation {generation} committed but UUID index snapshot refresh failed: {error}"
                )))
            }
        }
    }

    fn record_uuid_append_work(&mut self, metrics: &crate::UuidIndexAppendMetrics) {
        let work = &mut self.topology_work;
        work.uuid_input_records = work
            .uuid_input_records
            .saturating_add(metrics.input_records);
        work.uuid_prior_topology_rows_decoded = work
            .uuid_prior_topology_rows_decoded
            .saturating_add(metrics.prior_topology_rows_decoded);
        work.uuid_physical_bytes_written = work
            .uuid_physical_bytes_written
            .saturating_add(metrics.physical_bytes_written);
        work.uuid_write_blocks = work.uuid_write_blocks.saturating_add(metrics.write_blocks);
        work.uuid_write_bytes = work.uuid_write_bytes.saturating_add(metrics.write_bytes);
        work.uuid_peak_buffered_records = work
            .uuid_peak_buffered_records
            .max(u64::try_from(metrics.peak_buffered_records).unwrap_or(u64::MAX));
        work.uuid_peak_buffered_bytes = work
            .uuid_peak_buffered_bytes
            .max(u64::try_from(metrics.peak_buffered_bytes).unwrap_or(u64::MAX));
        work.uuid_validation_blocks = work
            .uuid_validation_blocks
            .saturating_add(metrics.validation_scan_blocks);
        work.uuid_validation_bytes = work
            .uuid_validation_bytes
            .saturating_add(metrics.validation_scan_bytes);
        work.uuid_validation_random_seeks = work
            .uuid_validation_random_seeks
            .saturating_add(metrics.validation_random_seeks);
    }

    fn record_v4_ordinal_append_work(
        &mut self,
        metrics: &crate::uuid_membership::V4OrdinalAppendMetrics,
    ) {
        let work = &mut self.topology_work;
        work.uuid_input_records = work
            .uuid_input_records
            .saturating_add(metrics.input_identities)
            .saturating_add(metrics.input_tombstones);
        work.uuid_prior_topology_rows_decoded = work
            .uuid_prior_topology_rows_decoded
            .saturating_add(metrics.prior_topology_rows_decoded);
        work.uuid_physical_bytes_written = work
            .uuid_physical_bytes_written
            .saturating_add(metrics.physical_bytes_written);
        work.uuid_write_blocks = work.uuid_write_blocks.saturating_add(metrics.write_blocks);
        work.uuid_write_bytes = work.uuid_write_bytes.saturating_add(metrics.write_bytes);
        work.uuid_peak_buffered_bytes = work
            .uuid_peak_buffered_bytes
            .max(u64::try_from(metrics.peak_buffer_bytes).unwrap_or(u64::MAX));
        work.uuid_validation_blocks = work
            .uuid_validation_blocks
            .saturating_add(metrics.sequential_read_blocks);
        work.uuid_validation_bytes = work
            .uuid_validation_bytes
            .saturating_add(metrics.sequential_read_bytes);
        work.uuid_validation_random_seeks = work
            .uuid_validation_random_seeks
            .saturating_add(metrics.per_record_seeks);
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
        let result = (|| {
            let topology_pending = !self.nodes.is_empty() || !self.edges.is_empty();
            self.flush_nodes(staged)?;
            self.flush_edges(staged)?;
            if topology_pending {
                self.stage_surrogate_tails(staged)?;
            }
            self.flush_properties(staged)?;
            self.flush_edge_properties(staged)?;
            Ok(())
        })();
        // Success releases encoded rows; failure may have consumed only a
        // prefix because this legacy writer is not reusable after staging
        // failure. In either case accounting follows the exact retained state.
        self.refresh_topology_charge();
        result
    }

    fn record_topology_shard(
        &mut self,
        staged: &RewriteBatch,
        path: &Path,
        rows: u64,
    ) -> Result<(), GfError> {
        let bytes = staged
            .staged_temp(path)
            .ok_or_else(|| GfError::Storage("staged topology shard is missing".into()))?
            .metadata()
            .map_err(|error| io_err(&error))?
            .len();
        self.topology_work.input_rows = self.topology_work.input_rows.saturating_add(rows);
        self.topology_work.rows_encoded = self.topology_work.rows_encoded.saturating_add(rows);
        self.topology_work.shard_count = self.topology_work.shard_count.saturating_add(1);
        self.topology_work.output_bytes = self.topology_work.output_bytes.saturating_add(bytes);
        self.topology_work.new_rows_written =
            self.topology_work.new_rows_written.saturating_add(rows);
        Ok(())
    }

    fn stage_surrogate_tails(&self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        let batch = RecordBatch::try_new(
            surrogate_tails_schema(),
            vec![
                Arc::new(UInt64Array::from(vec![self.next_node_id.saturating_sub(1)])),
                Arc::new(UInt64Array::from(vec![self.next_edge_id.saturating_sub(1)])),
            ],
        )
        .map_err(pq_err)?;
        staged.restage(
            &self.dir.join(SURROGATE_TAILS_FILE),
            surrogate_tails_schema(),
            &batch,
        )
    }

    fn flush_nodes(&mut self, staged: &mut RewriteBatch) -> Result<(), GfError> {
        if self.nodes.is_empty() {
            return Ok(());
        }
        let topology = self.dir.join("topology");
        fs::create_dir_all(&topology).map_err(|e| io_err(&e))?;

        let batch = self.pending_nodes_batch()?;

        let legacy = topology.join("nodes.parquet");
        let path = if !legacy.exists() && !topology.join("nodes").exists() {
            legacy.clone()
        } else {
            let first = self.nodes.first().map_or(0, |row| row.node_id);
            let last = self.nodes.last().map_or(first, |row| row.node_id);
            topology
                .join("nodes")
                .join(format!("{first:020}-{last:020}.parquet"))
        };
        if path != legacy && (path.exists() || staged.staged_temp(&path).is_some()) {
            return Err(GfError::Storage(
                "node shard surrogate range already exists".into(),
            ));
        }
        staged.stage(&path, TOPOLOGY_NODES_SCHEMA.clone(), &batch)?;
        crate::io_stats::record_topology_rewrite(0, batch.num_rows() as u64);
        self.record_topology_shard(staged, &path, batch.num_rows() as u64)?;
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
            // Every append becomes one immutable bounded fragment. Existing
            // fragments are neither decoded nor re-encoded, so aggregate
            // topology work is linear in accepted edge rows (#901). The
            // surrogate range makes the name deterministic and collision-safe
            // for monotonic writer sessions.
            let first = rows.first().map_or(0, |row| row.edge_id);
            let last = rows.last().map_or(first, |row| row.edge_id);
            let component = staged.route_component(&self.dir, &stem)?;
            let existing = crate::mutator::edge_parquet_files(&self.dir, Some(&component))?;
            let path = if existing.is_empty() {
                edges_dir.join(format!("{component}.parquet"))
            } else {
                edges_dir
                    .join(&component)
                    .join(format!("{first:020}-{last:020}.parquet"))
            };
            if path.exists() || staged.staged_temp(&path).is_some() {
                return Err(GfError::Storage(
                    "edge shard surrogate range already exists".into(),
                ));
            }
            staged.stage(&path, schema, &batch)?;
            crate::io_stats::record_topology_rewrite(0, batch.num_rows() as u64);
            self.record_topology_shard(staged, &path, batch.num_rows() as u64)?;
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
            let (rows, authority, authority_generation_uuid) =
                complete_node_property_window(staged, &self.dir, &stem, new_rows)?;
            let (inferred, _) = build_property_columns(&stem, &rows)?;
            let inferred = self.authenticated_route_schema(Arc::new(inferred), &stem);
            let schema = merge_property_write_schema(
                crate::PropertyRouteKind::Node,
                &stem,
                inferred,
                authority,
            )?;
            let cols =
                property_rows_batch_with_schema(schema.as_ref(), NODE_PROPERTY_UUID_FIELD, &rows)?
                    .columns()
                    .to_vec();
            stage_property_fragment(
                staged,
                &self.dir,
                PropertyFragmentInput {
                    kind: crate::property_overlay::PropertyRouteKind::Node,
                    route: &stem,
                    schema: &schema,
                    input_schema: schema.as_ref(),
                    columns: cols,
                    tombstone: false,
                    authority: authority_generation_uuid,
                },
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
            let (rows, authority, authority_generation_uuid) =
                complete_edge_property_window(staged, &self.dir, &stem, new_rows)?;
            let (inferred, _) = build_property_columns_keyed(
                EDGE_PROPERTY_UUID_FIELD,
                "graphforge.rel_type",
                &stem,
                &rows,
            )?;
            let inferred = self.authenticated_route_schema(Arc::new(inferred), &stem);
            let schema = merge_property_write_schema(
                crate::PropertyRouteKind::Edge,
                &stem,
                inferred,
                authority,
            )?;
            let cols =
                property_rows_batch_with_schema(schema.as_ref(), EDGE_PROPERTY_UUID_FIELD, &rows)?
                    .columns()
                    .to_vec();
            stage_property_fragment(
                staged,
                &self.dir,
                PropertyFragmentInput {
                    kind: crate::property_overlay::PropertyRouteKind::Edge,
                    route: &stem,
                    schema: &schema,
                    input_schema: schema.as_ref(),
                    columns: cols,
                    tombstone: false,
                    authority: authority_generation_uuid,
                },
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

// ---------------------------------------------------------------------------
// Property schema inference
// ---------------------------------------------------------------------------

/// Decode every persisted node-property row while retaining its canonical
/// [`IrLiteral`] type. This is the bounded base decoder used by authoritative
/// graph-delta replay; callers must apply their own aggregate replay budget.
pub(crate) fn read_all_node_properties(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
) -> Result<Vec<TypedPropertyRow>, GfError> {
    let mut rows = Vec::new();
    for stem in inventory.routes(crate::PropertyRouteKind::Node) {
        let batches =
            crate::catalog::read_properties_from_inventory(dir, inventory, stem).map_err(pq_err)?;
        rows.extend(
            decode_property_rows(&batches)?
                .into_iter()
                .map(|row| (stem.to_owned(), row.node_uuid, row.props)),
        );
    }
    Ok(rows)
}

/// Edge-property analogue of [`read_all_node_properties`].
pub(crate) fn read_all_edge_properties(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
) -> Result<Vec<TypedPropertyRow>, GfError> {
    let mut rows = Vec::new();
    for stem in inventory.routes(crate::PropertyRouteKind::Edge) {
        let batches = crate::catalog::read_edge_properties_from_inventory(dir, inventory, stem)
            .map_err(pq_err)?;
        rows.extend(
            decode_edge_property_rows(&batches)?
                .into_iter()
                .map(|row| (stem.to_owned(), row.edge_uuid, row.props)),
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

/// Read typed node rows through an explicitly admitted generation inventory.
pub fn read_node_property_rows_from_inventory(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    route: &str,
) -> Result<HashMap<[u8; 16], HashMap<String, IrLiteral>>, GfError> {
    let mut rows = HashMap::new();
    crate::catalog::visit_property_overlay_batched_with_inventory(
        dir,
        Some(inventory),
        route,
        false,
        8_192,
        |batch| {
            let decoded = decode_property_rows(std::slice::from_ref(batch)).map_err(|error| {
                datafusion::common::DataFusionError::Execution(error.to_string())
            })?;
            rows.extend(decoded.into_iter().map(|row| (row.node_uuid, row.props)));
            Ok(true)
        },
    )
    .map_err(pq_err)?;
    Ok(rows)
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
    let inventory = crate::property_overlay::authenticated_property_inventory_for_rewrite(
        dir,
        &RewriteBatch::new(),
    )?;
    count_entity_properties_from_inventory(dir, &inventory, targets, is_edge)
}

/// Count selected entity properties through the caller's admitted route inventory.
pub fn count_entity_properties_from_inventory<S: std::hash::BuildHasher>(
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    targets: &HashSet<[u8; 16], S>,
    is_edge: bool,
) -> Result<u64, GfError> {
    let mut count = 0u64;
    let kind = if is_edge {
        crate::PropertyRouteKind::Edge
    } else {
        crate::PropertyRouteKind::Node
    };
    let stems = inventory.routes(kind);
    for stem in stems {
        let batches = if is_edge {
            crate::catalog::read_edge_properties_from_inventory(dir, inventory, stem)
        } else {
            crate::catalog::read_properties_from_inventory(dir, inventory, stem)
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

// ---------------------------------------------------------------------------
// Parquet write helper
// ---------------------------------------------------------------------------

use crate::staging::RewriteBatch;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;

#[cfg(test)]
mod promotion_full_width_tests {
    use super::*;
    use arrow::array::UInt64Array;
    use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};

    #[test]
    fn same_name_promotion_preserves_full_width_authorities_and_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = graphforge_ir::RuntimeCatalog::new();
        let runtime_id = catalog.intern_label("Person").unwrap();
        catalog.intern_relation_type("KNOWS").unwrap();
        let label = graphforge_value::EntityTypeId::runtime(runtime_id);
        let left = Uuid::from_u128(1229801);
        let right = Uuid::from_u128(1229802);
        let edge = Uuid::from_u128(1229803);
        let node_base = u64::MAX - 8;
        let edge_base = u64::from(u32::MAX) + 7;
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1).unwrap();
        writer.next_node_id = node_base;
        writer.next_edge_id = edge_base;
        assert_eq!(writer.create_node(left, label).unwrap(), node_base);
        assert_eq!(writer.create_node(right, label).unwrap(), node_base + 1);
        assert_eq!(
            writer.create_edge(edge, "KNOWS", &left, &right).unwrap(),
            edge_base
        );
        writer.flush().unwrap();
        drop(writer);
        crate::runtime_entity_labels::persist_runtime_catalog(dir.path(), &catalog).unwrap();
        let document = OntologyLoader::load_yaml("ontology_id: wide\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types:\n  - name: KNOWS\n    src: Person\n    dst: Person\n".as_bytes()).unwrap();
        let ontology = OntologyHandle::new(OntologyCompiler::compile(&document).unwrap());
        crate::promote_runtime_graph_for_ontology(dir.path(), &ontology, &catalog).unwrap();
        let mut membership = crate::UuidMembershipIndex::open(dir.path()).unwrap();
        assert_eq!(
            membership.lookup_node_surrogates(&[left, right]).unwrap().0,
            [Some(node_base), Some(node_base + 1)]
        );
        drop(membership);
        let inventory =
            crate::property_overlay::authenticated_property_inventory(dir.path()).unwrap();
        let batches =
            crate::read_edges_from_inventory(&inventory, "KNOWS", OntologyMode::Strict).unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let batch = batches.iter().find(|batch| batch.num_rows() != 0).unwrap();
        for (name, expected) in [("edge_uuid", edge), ("src_uuid", left), ("dst_uuid", right)] {
            assert_eq!(
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    .unwrap()
                    .value(0),
                expected.as_bytes()
            );
        }
        for (name, expected) in [
            ("edge_id", edge_base),
            ("src_id", node_base),
            ("dst_id", node_base + 1),
        ] {
            assert_eq!(
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        let created = Uuid::from_u128(1229804);
        let next_edge = Uuid::from_u128(1229805);
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Advisory, 2).unwrap();
        writer.register_existing_endpoints(&[left]).unwrap();
        assert_eq!(
            writer
                .create_node(
                    created,
                    graphforge_value::EntityTypeId::ontology(
                        ontology.entity_type_id("Person").unwrap()
                    )
                    .unwrap()
                )
                .unwrap(),
            node_base + 2
        );
        assert_eq!(
            writer
                .create_edge(next_edge, "KNOWS", &left, &created)
                .unwrap(),
            edge_base + 1
        );
        writer.flush().unwrap();
        drop(writer);
        let mut membership = crate::UuidMembershipIndex::open(dir.path()).unwrap();
        assert_eq!(
            membership
                .lookup_node_surrogates(&[left, right, created])
                .unwrap()
                .0,
            [Some(node_base), Some(node_base + 1), Some(node_base + 2)]
        );
    }
}

mod property_codec;
mod property_mutation;
mod replay_properties;
mod replay_topology;

use property_codec::ColType;
use property_codec::build_property_array;
use property_codec::build_property_columns;
use property_codec::build_property_columns_keyed;
use property_codec::col_type_from_field;
use property_codec::decode_edge_property_rows;
pub(crate) use property_codec::decode_property_batch;
use property_codec::decode_property_rows;
pub use property_codec::decode_property_value;
pub use property_codec::decode_spatial_property_value;
pub(crate) use property_codec::heterogeneous_scalar_fields;
use property_codec::property_rows_batch_with_schema;
pub(crate) use property_codec::property_snapshots_to_batch;
use property_codec::reject_map_property_value;
pub(crate) use property_codec::validate_property_values;
pub use property_mutation::NodePropertySetCounts;
use property_mutation::PropertyFragmentInput;
use property_mutation::complete_edge_property_window;
use property_mutation::complete_node_property_window;
pub(crate) use property_mutation::edge_property_snapshots_batch;
use property_mutation::merge_property_write_schema;
pub use property_mutation::remove_edge_properties;
pub use property_mutation::remove_node_properties;
pub(crate) use property_mutation::seal_property_windows;
pub use property_mutation::set_edge_properties_rewrite;
pub use property_mutation::set_node_properties;
pub(crate) use property_mutation::stage_promoted_properties;
use property_mutation::stage_property_fragment;
pub use property_mutation::stage_property_tombstones_authenticated;
pub use property_mutation::stage_remove_edge_properties;
pub use property_mutation::stage_remove_edge_properties_authenticated;
pub use property_mutation::stage_remove_node_properties;
pub use property_mutation::stage_remove_node_properties_authenticated;
pub use property_mutation::stage_set_edge_properties;
pub use property_mutation::stage_set_edge_properties_authenticated;
pub use property_mutation::stage_set_node_properties;
pub use property_mutation::stage_set_node_properties_authenticated;
pub use property_mutation::stage_set_node_properties_authenticated_with_counts;
use replay_properties::replace_private_replay_route_table;
use replay_properties::stream_replay_properties;
pub(crate) use replay_topology::ReplayNodeSpoolEvidence;
use replay_topology::stream_replay_edges;
use replay_topology::stream_replay_nodes;
use replay_topology::validate_replay_edge_endpoints;
