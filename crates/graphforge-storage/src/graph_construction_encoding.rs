//! Publication-independent canonical encoding for sealed construction shapes.
//!
//! This layer deliberately stops before graph-object installation or `CURRENT`.
//! It converts the shaper's authenticated, UUID-ordered streams into the exact
//! ordinary storage schemas and prepares a streamed UUID-membership manifest.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Component, Path};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_filesystem::{StableDirectory, file_identity};
use graphforge_ir::runtime_entity_type_id;
use graphforge_ir::{CompositionBindingContext, SymbolBinding};
use graphforge_ontology::{QualifiedSymbol, SymbolKind};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_construction::{
    ConstructionSemanticAuthority, ConstructionShape, CountingChunkReader,
    GraphConstructionBudgets, IoCounter,
};
use crate::schemas::{
    TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, uuid_field, with_semantic_route_metadata,
};
use crate::uuid_membership::{
    AuthenticatedUuidIndexSnapshot, ConstructionIndexOutput, ConstructionIndexReference,
};
use crate::{SemanticRouteKind, SemanticStorageBindings};

const ENCODED_ROOT: &str = "encoded-v1";
const INVENTORY: &str = "inventory.json";
const MAX_INVENTORY_BYTES: u64 = 16 << 20;
const IDENTITY_WIDTH: usize = 32;
const NODE_DETAIL_WIDTH: usize = 272;
const EDGE_DETAIL_WIDTH: usize = 304;
const RESOLVED_ENDPOINT_WIDTH: usize = 32;
const COPY_BUFFER_BYTES: usize = 1 << 20;

struct CountingWriter {
    inner: File,
    counter: IoCounter,
}

struct CountingInput<R> {
    inner: R,
    counter: IoCounter,
}

impl<R: Read> Read for CountingInput<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.counter.account(read);
        Ok(read)
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.counter.account(written);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct EncodingRowCursor {
    reader: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>,
    batch: Option<RecordBatch>,
    row: usize,
    batch_sequence: u64,
    counter: IoCounter,
    counter_accounted: bool,
}

impl EncodingRowCursor {
    fn current_uuid(
        &mut self,
        uuid_name: &str,
        evidence: &mut GraphConstructionEncodingEvidence,
    ) -> Result<Option<[u8; 16]>, GfError> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                let values = required_uuid(batch, uuid_name)?;
                return Ok(Some(values.value(self.row).try_into().expect("fixed UUID")));
            }
            self.batch = self.reader.next().transpose().map_err(storage)?;
            self.row = 0;
            self.batch_sequence = self.batch_sequence.saturating_add(1);
            if self.batch.is_none() {
                if !self.counter_accounted {
                    let (bytes, operations) = self.counter.values();
                    evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
                    evidence.input_read_operations =
                        evidence.input_read_operations.saturating_add(operations);
                    self.counter_accounted = true;
                }
                return Ok(None);
            }
        }
    }
}

#[derive(Clone)]
struct MergedEncodingRow {
    source: usize,
    batch_sequence: u64,
    batch: RecordBatch,
    row: usize,
}

type EncodingRowHeap = BinaryHeap<(Reverse<[u8; 16]>, Reverse<usize>)>;

fn open_merged_rows(
    source: &StableDirectory,
    names: &[String],
    uuid_name: &str,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(Vec<EncodingRowCursor>, EncodingRowHeap), GfError> {
    if names.len() > budgets.max_schema_groups {
        return Err(storage(
            "encoded schema registry exceeds its cardinality cap",
        ));
    }
    let mut cursors = Vec::with_capacity(names.len());
    for name in names {
        let file = source.open_child_file(OsStr::new(name)).map_err(storage)?;
        let counter = IoCounter::default();
        let reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
            file,
            counter: counter.clone(),
        })
        .map_err(storage)?
        .with_batch_size(budgets.max_batch_rows)
        .build()
        .map_err(storage)?;
        cursors.push(EncodingRowCursor {
            reader: Box::new(reader),
            batch: None,
            row: 0,
            batch_sequence: 0,
            counter,
            counter_accounted: false,
        });
    }
    let mut heap = BinaryHeap::new();
    for (index, cursor) in cursors.iter_mut().enumerate() {
        if let Some(uuid) = cursor.current_uuid(uuid_name, evidence)? {
            heap.push((Reverse(uuid), Reverse(index)));
        }
    }
    Ok((cursors, heap))
}

fn next_merged_window(
    cursors: &mut [EncodingRowCursor],
    heap: &mut BinaryHeap<(Reverse<[u8; 16]>, Reverse<usize>)>,
    uuid_name: &str,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<Vec<MergedEncodingRow>, GfError> {
    let mut rows = Vec::with_capacity(budgets.max_batch_rows);
    let mut retained_bytes = 0_usize;
    let mut previous = None;
    while rows.len() < budgets.max_batch_rows {
        let Some((Reverse(uuid), Reverse(source))) = heap.pop() else {
            break;
        };
        if previous.is_some_and(|prior| prior >= uuid) {
            return Err(storage(
                "heterogeneous row streams are not globally unique and sorted",
            ));
        }
        previous = Some(uuid);
        let cursor = &mut cursors[source];
        let batch = cursor.batch.as_ref().expect("heap row has batch");
        if rows.iter().all(|row: &MergedEncodingRow| {
            row.source != source || row.batch_sequence != cursor.batch_sequence
        }) {
            account_batch(batch, budgets, evidence)?;
            retained_bytes = retained_bytes.saturating_add(batch.get_array_memory_size());
            if !rows.is_empty() && retained_bytes > budgets.max_batch_bytes {
                heap.push((Reverse(uuid), Reverse(source)));
                break;
            }
        }
        rows.push(MergedEncodingRow {
            source,
            batch_sequence: cursor.batch_sequence,
            batch: batch.clone(),
            row: cursor.row,
        });
        cursor.row += 1;
        if let Some(next) = cursor.current_uuid(uuid_name, evidence)? {
            heap.push((Reverse(next), Reverse(source)));
        }
        if rows.len().is_multiple_of(4096) && cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
    }
    evidence.peak_batch_rows = evidence.peak_batch_rows.max(rows.len() as u64);
    evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(retained_bytes as u64);
    Ok(rows)
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("graph construction encoding: {error}"))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// One private canonical artifact authenticated by the completed inventory.
pub struct ConstructionEncodedArtifact {
    /// Normalized graph-root-relative path.
    pub path: String,
    /// Exact physical bytes.
    pub bytes: u64,
    /// Lowercase SHA-256 of the file.
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Exact authenticated parent object that a publisher must structurally retain.
pub struct ConstructionRetainedArtifact {
    /// Stable authenticated parent directory supplied to the publisher.
    pub source_root: String,
    /// Device identity of the authenticated parent directory.
    pub source_root_volume: u64,
    /// File identity of the authenticated parent directory.
    pub source_root_file_id: String,
    /// Parent-root-relative immutable object name.
    pub source_path: String,
    /// Device identity of the retained immutable object.
    pub source_volume: u64,
    /// File identity of the retained immutable object.
    pub source_file_id: String,
    /// Generation-root-relative target name for structural installation.
    pub target_path: String,
    /// Exact retained object length.
    pub bytes: u64,
    /// Exact retained object digest.
    pub sha256: String,
    /// Authenticated parent index manifest that authorized this reference.
    pub parent_manifest_sha256: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Measured bounded work for canonical encoding.
pub struct GraphConstructionEncodingEvidence {
    /// Shaped input bytes read sequentially.
    pub input_read_bytes: u64,
    /// Bounded input read operations.
    pub input_read_operations: u64,
    /// Canonical output bytes written.
    pub output_write_bytes: u64,
    /// Canonical output write operations.
    pub output_write_operations: u64,
    /// Completed file and directory durability barriers.
    pub fsync_operations: u64,
    /// Topology rows decoded from the retained parent. Always zero.
    pub prior_topology_rows_decoded: u64,
    /// Retained topology bytes copied. Always zero.
    pub retained_topology_bytes_copied: u64,
    /// Largest decoded Arrow window.
    pub peak_batch_rows: u64,
    /// Largest decoded Arrow window in bytes.
    pub peak_batch_bytes: u64,
    /// Largest number of simultaneously live shard writers. Always one.
    pub peak_open_writers: u64,
    /// New identity records streamed into the v3 index.
    pub membership_records: u64,
    /// New v3 membership bytes written.
    pub membership_write_bytes: u64,
    /// Retained v3 run descriptors structurally reused.
    pub retained_index_runs: u64,
    /// Retained v3 payload bytes read only for required binary-carry compaction.
    pub retained_index_payload_bytes: u64,
    /// Actual block reads performed by v3 construction encoding and carry.
    pub membership_read_operations: u64,
    /// Actual block writes, including outputs superseded by carry.
    pub membership_write_operations: u64,
    /// All v3 bytes written, including superseded carry inputs.
    pub membership_total_write_bytes: u64,
    /// v3 durability barriers.
    pub membership_fsync_operations: u64,
    /// Newly created immutable v3 runs, including superseded carry inputs.
    pub membership_created_runs: u64,
    /// Peak live v3 transform/merge buffer bytes.
    pub membership_peak_buffer_bytes: u64,
    /// Peak bytes in owned temporary v3 outputs.
    pub membership_peak_temporary_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Completed private canonical artifact inventory.
pub struct GraphConstructionEncoding {
    /// Directory below the private operation root containing this inventory.
    pub root: String,
    /// Generation the eventual publisher must bind.
    pub generation: u64,
    /// Physical routing authority pinned by the construction checkpoint.
    pub ontology_mode: OntologyMode,
    /// Exact compiled composition/physical binding authority, when present.
    pub semantic_authority_sha256: Option<String>,
    /// Shaper authority digest for normalized catalog inputs.
    pub shape_inputs_sha256: String,
    /// Sorted, unique canonical artifact records.
    pub artifacts: Vec<ConstructionEncodedArtifact>,
    /// Authenticated retained-parent objects required to assemble this graph.
    pub retained_artifacts: Vec<ConstructionRetainedArtifact>,
    /// Measured bounded work.
    pub evidence: GraphConstructionEncodingEvidence,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn encode(
    source: &StableDirectory,
    shape: &ConstructionShape,
    generation: u64,
    ontology_mode: OntologyMode,
    parent_index: Option<&AuthenticatedUuidIndexSnapshot>,
    semantic_authority: Option<&ConstructionSemanticAuthority>,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphConstructionEncoding, GfError> {
    if shape.ontology_mode != ontology_mode {
        return Err(storage(
            "shape ontology mode differs from session authority",
        ));
    }
    let semantic_digest = semantic_authority
        .map(ConstructionSemanticAuthority::digest)
        .transpose()?;
    if shape.semantic_authority_sha256 != semantic_digest {
        return Err(storage("shape semantic authority differs from session"));
    }
    if generation == 0 || generation <= shape.parent_topology_generation {
        return Err(storage("encoded generation is not newer than its parent"));
    }
    if cancelled() {
        return Err(storage("construction encoding cancelled"));
    }
    let output = source
        .create_child_directory(OsStr::new(ENCODED_ROOT))
        .map_err(storage)?;
    if let Some(existing) = read_inventory(&output)? {
        authenticate_inventory(&output, &existing)?;
        if existing.generation != generation
            || existing.ontology_mode != shape.ontology_mode
            || existing.semantic_authority_sha256 != shape.semantic_authority_sha256
            || existing.shape_inputs_sha256 != shape.runtime_catalog_inputs_sha256
        {
            return Err(storage("completed encoding belongs to another generation"));
        }
        return Ok(existing);
    }

    let mut evidence = GraphConstructionEncodingEvidence {
        peak_open_writers: 1,
        ..Default::default()
    };
    let label_ids = read_runtime_label_ids(source, &shape.runtime_catalog, budgets, &mut evidence)?;
    let semantic_context = semantic_authority
        .map(ConstructionSemanticAuthority::context)
        .transpose()?;
    let mut artifacts = Vec::new();

    encode_nodes(
        source,
        &output,
        shape,
        &label_ids,
        ontology_mode,
        semantic_context.as_ref(),
        semantic_authority.map(|authority| &authority.bindings),
        budgets,
        cancelled,
        &mut artifacts,
        &mut evidence,
    )?;
    encode_edges(
        source,
        &output,
        shape,
        ontology_mode,
        semantic_context.as_ref(),
        semantic_authority.map(|authority| &authority.bindings),
        budgets,
        cancelled,
        &mut artifacts,
        &mut evidence,
    )?;
    copy_artifact(
        source,
        &shape.runtime_catalog,
        &output,
        "topology/runtime_catalog.parquet",
        &mut artifacts,
        &mut evidence,
    )?;
    write_surrogate_tails(
        &output,
        shape.max_node_surrogate,
        shape.max_edge_surrogate,
        &mut artifacts,
        &mut evidence,
    )?;

    let index = crate::uuid_membership::encode_construction_index(
        source,
        &shape.identities,
        &output,
        generation,
        shape.parent_topology_generation,
        parent_index,
        shape.node_count,
        shape.edge_count,
        cancelled,
    )?;
    evidence.membership_records = index.input_records;
    evidence.membership_write_bytes = index.write_bytes;
    evidence.membership_total_write_bytes = index.write_bytes;
    evidence.membership_read_operations = index.read_operations;
    evidence.membership_write_operations = index.write_operations;
    evidence.membership_fsync_operations = index.fsync_operations;
    evidence.membership_created_runs = index.created_runs;
    evidence.membership_peak_buffer_bytes = index.peak_buffer_bytes;
    evidence.membership_peak_temporary_bytes = index.peak_temporary_bytes;
    evidence.retained_index_runs = index.retained_runs;
    evidence.retained_index_payload_bytes = index.retained_payload_bytes;
    let retained_artifacts = index
        .retained_references
        .into_iter()
        .map(retained_artifact)
        .collect();
    artifacts.extend(index.artifacts.into_iter().map(index_artifact));

    artifacts.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    if artifacts
        .windows(2)
        .any(|pair| pair[0].path == pair[1].path)
    {
        return Err(storage("canonical encoding contains duplicate paths"));
    }
    let completed = GraphConstructionEncoding {
        root: ENCODED_ROOT.to_owned(),
        generation,
        ontology_mode: shape.ontology_mode,
        semantic_authority_sha256: shape.semantic_authority_sha256.clone(),
        shape_inputs_sha256: shape.runtime_catalog_inputs_sha256.clone(),
        artifacts,
        retained_artifacts,
        evidence,
    };
    install_json(&output, INVENTORY, &completed)?;
    authenticate_inventory(&output, &completed)?;
    Ok(completed)
}

fn retained_artifact(value: ConstructionIndexReference) -> ConstructionRetainedArtifact {
    ConstructionRetainedArtifact {
        source_root: value.source_root,
        source_root_volume: value.source_root_volume,
        source_root_file_id: value.source_root_file_id,
        source_path: value.source_path,
        source_volume: value.source_volume,
        source_file_id: value.source_file_id,
        target_path: value.target_path,
        bytes: value.bytes,
        sha256: value.sha256,
        parent_manifest_sha256: value.parent_manifest_sha256,
    }
}

fn index_artifact(value: ConstructionIndexOutput) -> ConstructionEncodedArtifact {
    ConstructionEncodedArtifact {
        path: format!(".graphforge-cache/uuid-membership/{}", value.name),
        bytes: value.bytes,
        sha256: value.sha256,
    }
}

#[derive(Clone)]
struct ResolvedOwner {
    input: String,
    symbol: Option<QualifiedSymbol>,
    topology_route: String,
    storage_id: Option<u32>,
}

fn resolve_owner(
    context: Option<&CompositionBindingContext>,
    bindings: Option<&SemanticStorageBindings>,
    symbol_kind: SymbolKind,
    route_kind: SemanticRouteKind,
    input: &str,
    runtime_route: &str,
) -> Result<ResolvedOwner, GfError> {
    let (Some(context), Some(bindings)) = (context, bindings) else {
        return Ok(ResolvedOwner {
            input: input.to_owned(),
            symbol: None,
            topology_route: runtime_route.to_owned(),
            storage_id: None,
        });
    };
    let (binding, _) = context
        .resolve(symbol_kind, input)
        .map_err(|error| storage(format!("semantic owner resolution failed: {error:?}")))?;
    match binding {
        SymbolBinding::Runtime { .. } => Ok(ResolvedOwner {
            input: input.to_owned(),
            symbol: None,
            topology_route: runtime_route.to_owned(),
            storage_id: None,
        }),
        SymbolBinding::Qualified(symbol) => {
            let physical = bindings
                .bindings
                .iter()
                .find(|candidate| candidate.route_kind == route_kind && candidate.symbol == symbol)
                .ok_or_else(|| storage("qualified owner lacks physical semantic binding"))?;
            Ok(ResolvedOwner {
                input: input.to_owned(),
                symbol: Some(symbol),
                topology_route: physical.route.clone(),
                storage_id: Some(physical.storage_id),
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn property_projection(
    input: &RecordBatch,
    required: usize,
    indexes: &[u32],
    owner: &ResolvedOwner,
    owner_kind: SymbolKind,
    route_kind: SemanticRouteKind,
    context: Option<&CompositionBindingContext>,
    bindings: Option<&SemanticStorageBindings>,
) -> Result<(String, Vec<usize>), GfError> {
    let mut fields = Vec::new();
    let mut route = None;
    for column_index in required..input.num_columns() {
        let column = input.column(column_index);
        if !indexes.iter().any(|index| !column.is_null(*index as usize)) {
            continue;
        }
        if let (Some(context), Some(bindings), Some(owner_symbol)) =
            (context, bindings, owner.symbol.as_ref())
        {
            let schema = input.schema();
            let field = schema.field(column_index);
            let (binding, _) = context
                .resolve_owned_property(owner_kind, &owner.input, field.name())
                .map_err(|error| {
                    storage(format!("semantic property resolution failed: {error:?}"))
                })?;
            let SymbolBinding::Qualified(symbol) = binding else {
                return Err(storage(
                    "qualified owner property resolved to runtime authority",
                ));
            };
            let physical = bindings
                .bindings
                .iter()
                .find(|candidate| {
                    candidate.route_kind == route_kind
                        && candidate.symbol == symbol
                        && candidate.owner.as_ref() == Some(owner_symbol)
                })
                .ok_or_else(|| storage("qualified property lacks owner-bound physical route"))?;
            if route.as_ref().is_some_and(|known| known != &physical.route) {
                return Err(storage(
                    "one semantic owner maps to multiple property routes",
                ));
            }
            route = Some(physical.route.clone());
        }
        fields.push(column_index);
    }
    Ok((
        route.unwrap_or_else(|| owner.topology_route.clone()),
        fields,
    ))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_nodes(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    label_ids: &BTreeMap<String, u32>,
    ontology_mode: OntologyMode,
    semantic_context: Option<&CompositionBindingContext>,
    semantic_bindings: Option<&SemanticStorageBindings>,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    if shape.node_rows.is_empty() {
        return Ok(());
    }
    let details_name = shape
        .node_details
        .as_deref()
        .ok_or_else(|| storage("node rows lack canonical details"))?;
    let mut identities = FixedReader::<IDENTITY_WIDTH>::open(source, &shape.identities, evidence)?;
    let mut details = FixedReader::<NODE_DETAIL_WIDTH>::open(source, details_name, evidence)?;
    let (mut row_cursors, mut row_heap) =
        open_merged_rows(source, &shape.node_rows, "node_uuid", budgets, evidence)?;
    let mut ordinal = 0_u64;
    loop {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let merged = next_merged_window(
            &mut row_cursors,
            &mut row_heap,
            "node_uuid",
            budgets,
            cancelled,
            evidence,
        )?;
        if merged.is_empty() {
            break;
        }
        let mut out_uuid = Vec::with_capacity(merged.len());
        let mut out_id = Vec::with_capacity(merged.len());
        let mut out_type = Vec::with_capacity(merged.len());
        let mut owners = BTreeMap::<String, ResolvedOwner>::new();
        let mut groups = BTreeMap::<(usize, u64, String), (RecordBatch, Vec<u32>)>::new();
        for merged_row in &merged {
            let input = &merged_row.batch;
            let row = merged_row.row;
            let uuids = required_uuid(input, "node_uuid")?;
            let labels = required_string(input, "label")?;
            let uuid: [u8; 16] = uuids.value(row).try_into().expect("fixed UUID");
            let identity = next_kind(&mut identities, 0)?
                .ok_or_else(|| storage("node identity stream ended early"))?;
            let detail = details
                .next()?
                .ok_or_else(|| storage("node detail stream ended early"))?;
            if identity[..16] != uuid || detail[..16] != uuid || identity[17] != 0 {
                return Err(storage("node row/detail/identity streams differ"));
            }
            let label_len = usize::from(detail[16]);
            let label = std::str::from_utf8(&detail[17..17 + label_len]).map_err(storage)?;
            if label != labels.value(row) {
                return Err(storage("node detail label differs from normalized row"));
            }
            let runtime_route = if ontology_mode == OntologyMode::Exploratory {
                "_untyped"
            } else {
                label
            };
            let owner = if let Some(owner) = owners.get(label) {
                owner.clone()
            } else {
                let owner = resolve_owner(
                    semantic_context,
                    semantic_bindings,
                    SymbolKind::Entity,
                    SemanticRouteKind::Entity,
                    label,
                    runtime_route,
                )?;
                owners.insert(label.to_owned(), owner.clone());
                owner
            };
            let type_id = match owner.storage_id {
                Some(storage_id) => storage_id,
                None => *label_ids
                    .get(label)
                    .ok_or_else(|| storage("node label is absent from runtime catalog"))?,
            };
            out_uuid.push(uuid);
            out_id.push(u64::from_be_bytes(
                identity[24..32].try_into().expect("fixed"),
            ));
            out_type.push(type_id);
            groups
                .entry((
                    merged_row.source,
                    merged_row.batch_sequence,
                    label.to_owned(),
                ))
                .or_insert_with(|| (input.clone(), Vec::new()))
                .1
                .push(u32::try_from(row).map_err(storage)?);
        }
        if out_id.is_empty() {
            continue;
        }
        let canonical = node_batch(
            &out_uuid,
            &out_id,
            &out_type,
            shape.runtime_catalog_now_micros,
        )?;
        let first = *out_id.first().expect("nonempty");
        let last = *out_id.last().expect("nonempty");
        let path = format!("topology/nodes/{first:020}-{last:020}.parquet");
        artifacts.push(write_parquet(output, &path, &canonical, evidence)?);
        for ((_source, _batch_sequence, label), (input, indexes)) in groups {
            if input.num_columns() == 2 {
                continue;
            }
            let owner = owners
                .get(&label)
                .ok_or_else(|| storage("node owner resolution was lost"))?;
            let (route, fields) = property_projection(
                &input,
                2,
                &indexes,
                owner,
                SymbolKind::Entity,
                SemanticRouteKind::NodeProperty,
                semantic_context,
                semantic_bindings,
            )?;
            if fields.is_empty() {
                continue;
            }
            let property = property_batch(
                &input,
                "node_uuid",
                "graphforge.entity_type",
                &route,
                &indexes,
                &fields,
            )?;
            let property = if owner.symbol.is_some() {
                with_route_metadata_batch(
                    &property,
                    &route,
                    semantic_context
                        .expect("qualified owner has context")
                        .fingerprint(),
                )?
            } else {
                property
            };
            let path = format!(
                "properties/{route}/{:020}-{ordinal:020}.parquet",
                shape.parent_topology_generation + 1
            );
            artifacts.push(write_parquet(output, &path, &property, evidence)?);
            ordinal = ordinal.saturating_add(1);
        }
    }
    if details.next()?.is_some() || next_kind(&mut identities, 0)?.is_some() {
        return Err(storage("node streams contain unconsumed rows"));
    }
    identities.account(evidence);
    details.account(evidence);
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_edges(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    ontology_mode: OntologyMode,
    semantic_context: Option<&CompositionBindingContext>,
    semantic_bindings: Option<&SemanticStorageBindings>,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    if shape.edge_rows.is_empty() {
        return Ok(());
    }
    let details_name = shape
        .edge_details
        .as_deref()
        .ok_or_else(|| storage("edge rows lack canonical details"))?;
    let endpoints_name = shape
        .edge_endpoints
        .as_deref()
        .ok_or_else(|| storage("edge rows lack resolved endpoints"))?;
    let mut identities = FixedReader::<IDENTITY_WIDTH>::open(source, &shape.identities, evidence)?;
    let mut details = FixedReader::<EDGE_DETAIL_WIDTH>::open(source, details_name, evidence)?;
    let mut endpoints =
        FixedReader::<RESOLVED_ENDPOINT_WIDTH>::open(source, endpoints_name, evidence)?;
    let (mut row_cursors, mut row_heap) =
        open_merged_rows(source, &shape.edge_rows, "edge_uuid", budgets, evidence)?;
    let mut ordinal = 0_u64;
    loop {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let merged = next_merged_window(
            &mut row_cursors,
            &mut row_heap,
            "edge_uuid",
            budgets,
            cancelled,
            evidence,
        )?;
        if merged.is_empty() {
            break;
        }
        let mut out_uuid = Vec::with_capacity(merged.len());
        let mut out_src = Vec::with_capacity(merged.len());
        let mut out_dst = Vec::with_capacity(merged.len());
        let mut out_id = Vec::with_capacity(merged.len());
        let mut out_src_id = Vec::with_capacity(merged.len());
        let mut out_dst_id = Vec::with_capacity(merged.len());
        let mut owners = BTreeMap::<String, ResolvedOwner>::new();
        let mut groups =
            BTreeMap::<(usize, u64, String), (RecordBatch, Vec<u32>, Vec<usize>)>::new();
        for (output_row, merged_row) in merged.iter().enumerate() {
            let input = &merged_row.batch;
            let row = merged_row.row;
            let uuids = required_uuid(input, "edge_uuid")?;
            let srcs = required_uuid(input, "source_uuid")?;
            let dsts = required_uuid(input, "target_uuid")?;
            let routes = required_string(input, "rel_type")?;
            let uuid: [u8; 16] = uuids.value(row).try_into().expect("fixed UUID");
            let identity = next_kind(&mut identities, 1)?
                .ok_or_else(|| storage("edge identity stream ended early"))?;
            let detail = details
                .next()?
                .ok_or_else(|| storage("edge detail stream ended early"))?;
            let source_endpoint = endpoints
                .next()?
                .ok_or_else(|| storage("edge endpoint stream ended early"))?;
            let target_endpoint = endpoints
                .next()?
                .ok_or_else(|| storage("edge endpoint stream ended early"))?;
            if identity[..16] != uuid
                || detail[..16] != uuid
                || identity[17] != 0
                || source_endpoint[..16] != uuid
                || target_endpoint[..16] != uuid
                || source_endpoint[16] != 0
                || target_endpoint[16] != 1
            {
                return Err(storage("edge row/detail/identity/endpoint streams differ"));
            }
            let route_len = usize::from(detail[48]);
            let route = std::str::from_utf8(&detail[49..49 + route_len]).map_err(storage)?;
            if route != routes.value(row)
                || detail[16..32] != srcs.value(row)[..]
                || detail[32..48] != dsts.value(row)[..]
            {
                return Err(storage("edge canonical detail differs from normalized row"));
            }
            out_uuid.push(uuid);
            out_src.push(srcs.value(row).try_into().expect("fixed UUID"));
            out_dst.push(dsts.value(row).try_into().expect("fixed UUID"));
            out_id.push(u64::from_be_bytes(
                identity[24..32].try_into().expect("fixed"),
            ));
            out_src_id.push(u64::from_be_bytes(
                source_endpoint[24..32].try_into().expect("fixed"),
            ));
            out_dst_id.push(u64::from_be_bytes(
                target_endpoint[24..32].try_into().expect("fixed"),
            ));
            if !owners.contains_key(route) {
                let runtime_route = if ontology_mode == OntologyMode::Exploratory {
                    "_exploratory"
                } else {
                    route
                };
                owners.insert(
                    route.to_owned(),
                    resolve_owner(
                        semantic_context,
                        semantic_bindings,
                        SymbolKind::Relation,
                        SemanticRouteKind::Relation,
                        route,
                        runtime_route,
                    )?,
                );
            }
            let group = groups
                .entry((
                    merged_row.source,
                    merged_row.batch_sequence,
                    route.to_owned(),
                ))
                .or_insert_with(|| (input.clone(), Vec::new(), Vec::new()));
            group.1.push(u32::try_from(row).map_err(storage)?);
            group.2.push(output_row);
        }
        if out_id.is_empty() {
            continue;
        }
        let canonical = edge_batch(
            &out_uuid,
            &out_src,
            &out_dst,
            &out_id,
            &out_src_id,
            &out_dst_id,
            shape.runtime_catalog_now_micros,
        )?;
        for ((_source, _batch_sequence, route), (input, indexes, output_indexes)) in groups {
            let output_indexes = output_indexes
                .into_iter()
                .map(|value| u32::try_from(value).map_err(storage))
                .collect::<Result<Vec<_>, _>>()?;
            let mut selected = select_rows(&canonical, &output_indexes)?;
            let owner = owners
                .get(&route)
                .ok_or_else(|| storage("edge owner resolution was lost"))?;
            let topology_route = if owner.symbol.is_none()
                && ontology_mode == OntologyMode::Exploratory
            {
                let routes = StringArray::from(vec![route.as_str(); selected.num_rows()]);
                let mut fields = selected
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect::<Vec<_>>();
                fields.push(Field::new("rel_type_name", DataType::Utf8, false));
                let mut columns = selected.columns().to_vec();
                columns.push(Arc::new(routes));
                selected =
                    RecordBatch::try_new(crate::schemas::EXPLORATORY_EDGE_SCHEMA.clone(), columns)
                        .map_err(storage)?;
                "_exploratory"
            } else {
                owner.topology_route.as_str()
            };
            if owner.symbol.is_some() {
                selected = with_route_metadata_batch(
                    &selected,
                    topology_route,
                    semantic_context
                        .expect("qualified owner has context")
                        .fingerprint(),
                )?;
            }
            let ids = selected
                .column_by_name("edge_id")
                .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| storage("canonical edge ids are incompatible"))?;
            let first = ids.value(0);
            let last = ids.value(ids.len() - 1);
            let path = format!("topology/edges/{topology_route}/{first:020}-{last:020}.parquet");
            artifacts.push(write_parquet(output, &path, &selected, evidence)?);
            if input.num_columns() > 4 {
                let (property_route, fields) = property_projection(
                    &input,
                    4,
                    &indexes,
                    owner,
                    SymbolKind::Relation,
                    SemanticRouteKind::EdgeProperty,
                    semantic_context,
                    semantic_bindings,
                )?;
                if fields.is_empty() {
                    continue;
                }
                let property = property_batch(
                    &input,
                    "edge_uuid",
                    "graphforge.rel_type",
                    &property_route,
                    &indexes,
                    &fields,
                )?;
                let property = if owner.symbol.is_some() {
                    with_route_metadata_batch(
                        &property,
                        &property_route,
                        semantic_context
                            .expect("qualified owner has context")
                            .fingerprint(),
                    )?
                } else {
                    property
                };
                let path = format!(
                    "edge_properties/{property_route}/{:020}-{ordinal:020}.parquet",
                    shape.parent_topology_generation + 1
                );
                artifacts.push(write_parquet(output, &path, &property, evidence)?);
                ordinal = ordinal.saturating_add(1);
            }
        }
    }
    if details.next()?.is_some()
        || endpoints.next()?.is_some()
        || next_kind(&mut identities, 1)?.is_some()
    {
        return Err(storage("edge streams contain unconsumed rows"));
    }
    identities.account(evidence);
    details.account(evidence);
    endpoints.account(evidence);
    Ok(())
}

fn next_kind(
    identities: &mut FixedReader<IDENTITY_WIDTH>,
    kind: u8,
) -> Result<Option<[u8; IDENTITY_WIDTH]>, GfError> {
    while let Some(record) = identities.next()? {
        if record[16] == kind {
            return Ok(Some(record));
        }
        if !matches!(record[16], 0 | 1) {
            return Err(storage("identity stream contains invalid kind"));
        }
    }
    Ok(None)
}

fn node_batch(
    uuids: &[[u8; 16]],
    ids: &[u64],
    types: &[u32],
    now: i64,
) -> Result<RecordBatch, GfError> {
    let nullable = ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
        types.iter().map(|value| Some([Some(*value)])),
    );
    let labels = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        nullable.offsets().clone(),
        nullable.values().clone(),
        None,
    );
    RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter().copied()).map_err(storage)?),
            Arc::new(UInt64Array::from(ids.to_vec())),
            Arc::new(UInt32Array::from(types.to_vec())),
            Arc::new(labels),
            Arc::new(TimestampMicrosecondArray::from(vec![now; ids.len()]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![now; ids.len()]).with_timezone("UTC")),
        ],
    )
    .map_err(storage)
}

fn edge_batch(
    uuids: &[[u8; 16]],
    srcs: &[[u8; 16]],
    dsts: &[[u8; 16]],
    ids: &[u64],
    src_ids: &[u64],
    dst_ids: &[u64],
    now: i64,
) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter().copied()).map_err(storage)?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(srcs.iter().copied()).map_err(storage)?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(dsts.iter().copied()).map_err(storage)?),
            Arc::new(UInt64Array::from(ids.to_vec())),
            Arc::new(UInt64Array::from(src_ids.to_vec())),
            Arc::new(UInt64Array::from(dst_ids.to_vec())),
            Arc::new(TimestampMicrosecondArray::from(vec![now; ids.len()]).with_timezone("UTC")),
        ],
    )
    .map_err(storage)
}

fn property_batch(
    input: &RecordBatch,
    uuid_name: &str,
    metadata_key: &str,
    owner: &str,
    indexes: &[u32],
    field_indexes: &[usize],
) -> Result<RecordBatch, GfError> {
    let indexes = UInt32Array::from(indexes.to_vec());
    let mut fields = vec![uuid_field(uuid_name)];
    fields.extend(
        field_indexes
            .iter()
            .map(|index| input.schema().field(*index).clone()),
    );
    let schema = Schema::new(fields).with_metadata(
        [(metadata_key.to_owned(), owner.to_owned())]
            .into_iter()
            .collect(),
    );
    let columns = std::iter::once(input.column(0))
        .chain(field_indexes.iter().map(|index| input.column(*index)))
        .map(|array| take(array.as_ref(), &indexes, None).map_err(storage))
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    RecordBatch::try_new(Arc::new(schema), columns).map_err(storage)
}

fn with_route_metadata_batch(
    batch: &RecordBatch,
    route: &str,
    fingerprint: &str,
) -> Result<RecordBatch, GfError> {
    let schema = with_semantic_route_metadata(batch.schema().as_ref(), route, fingerprint);
    RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).map_err(storage)
}

fn select_rows(batch: &RecordBatch, indexes: &[u32]) -> Result<RecordBatch, GfError> {
    let indexes = UInt32Array::from(indexes.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|array| take(array.as_ref(), &indexes, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(storage)
}

fn required_uuid<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| storage(format!("{name} is not FixedSizeBinary(16)")))
}

fn required_string<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| storage(format!("{name} is not Utf8")))
}

fn account_batch(
    batch: &RecordBatch,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let bytes = batch.get_array_memory_size();
    if batch.num_rows() > budgets.max_batch_rows || bytes > budgets.max_batch_bytes {
        return Err(storage("decoded canonical batch exceeds encoding budget"));
    }
    evidence.peak_batch_rows = evidence.peak_batch_rows.max(batch.num_rows() as u64);
    evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(bytes as u64);
    Ok(())
}

fn read_runtime_label_ids(
    source: &StableDirectory,
    name: &str,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<BTreeMap<String, u32>, GfError> {
    let file = source.open_child_file(OsStr::new(name)).map_err(storage)?;
    let counter = IoCounter::default();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file,
        counter: counter.clone(),
    })
    .map_err(storage)?
    .with_batch_size(budgets.max_batch_rows)
    .build()
    .map_err(storage)?;
    let mut labels = BTreeMap::new();
    for batch in &mut reader {
        let batch = batch.map_err(storage)?;
        account_batch(&batch, budgets, evidence)?;
        let kinds = required_string(&batch, "entry_kind")?;
        let names = required_string(&batch, "name")?;
        let ids = batch
            .column_by_name("runtime_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| storage("runtime catalog id is not UInt32"))?;
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "entity_type" {
                let tagged = runtime_entity_type_id(graphforge_ir::RuntimeTypeId(ids.value(row))).0;
                if labels.insert(names.value(row).to_owned(), tagged).is_some() {
                    return Err(storage("runtime catalog repeats an entity type"));
                }
            }
        }
    }
    let (bytes, operations) = counter.values();
    evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
    evidence.input_read_operations = evidence.input_read_operations.saturating_add(operations);
    Ok(labels)
}

fn write_surrogate_tails(
    output: &StableDirectory,
    max_node_id: u64,
    max_edge_id: u64,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("max_node_id", DataType::UInt64, false),
        Field::new("max_edge_id", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![max_node_id])),
            Arc::new(UInt64Array::from(vec![max_edge_id])),
        ],
    )
    .map_err(storage)?;
    artifacts.push(write_parquet(
        output,
        "topology/surrogate_tails.parquet",
        &batch,
        evidence,
    )?);
    Ok(())
}

fn write_parquet(
    root: &StableDirectory,
    relative: &str,
    batch: &RecordBatch,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<ConstructionEncodedArtifact, GfError> {
    let (directory, name) = directory_for(root, relative)?;
    let temporary = format!(".{}-{}.tmp", name, Uuid::new_v4().simple());
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let counter = IoCounter::default();
    let mut writer = ArrowWriter::try_new(
        CountingWriter {
            inner: file,
            counter: counter.clone(),
        },
        batch.schema(),
        None,
    )
    .map_err(storage)?;
    writer.write(batch).map_err(storage)?;
    writer.close().map_err(storage)?;
    let mut file = directory
        .open_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    file.sync_all().map_err(storage)?;
    let artifact = authenticate_file(relative, &mut file)?;
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(&name))
        .map_err(storage)?;
    directory.sync().map_err(storage)?;
    let (written, operations) = counter.values();
    evidence.output_write_bytes = evidence.output_write_bytes.saturating_add(written);
    evidence.output_write_operations = evidence.output_write_operations.saturating_add(operations);
    evidence.fsync_operations = evidence.fsync_operations.saturating_add(2);
    Ok(artifact)
}

fn copy_artifact(
    source: &StableDirectory,
    source_name: &str,
    output: &StableDirectory,
    relative: &str,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut input = BufReader::with_capacity(
        COPY_BUFFER_BYTES,
        source
            .open_child_file(OsStr::new(source_name))
            .map_err(storage)?,
    );
    let (directory, name) = directory_for(output, relative)?;
    let temporary = format!(".{}-{}.tmp", name, Uuid::new_v4().simple());
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = BufWriter::with_capacity(COPY_BUFFER_BYTES, file);
    let bytes = std::io::copy(&mut input, &mut writer).map_err(storage)?;
    writer.flush().map_err(storage)?;
    writer.get_ref().sync_all().map_err(storage)?;
    drop(writer);
    let mut file = directory
        .open_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let artifact = authenticate_file(relative, &mut file)?;
    if artifact.bytes != bytes {
        return Err(storage("copied canonical artifact length changed"));
    }
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(&name))
        .map_err(storage)?;
    directory.sync().map_err(storage)?;
    evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
    evidence.input_read_operations = evidence
        .input_read_operations
        .saturating_add(bytes.div_ceil(COPY_BUFFER_BYTES as u64));
    evidence.output_write_bytes = evidence.output_write_bytes.saturating_add(bytes);
    evidence.output_write_operations = evidence
        .output_write_operations
        .saturating_add(bytes.div_ceil(COPY_BUFFER_BYTES as u64));
    evidence.fsync_operations = evidence.fsync_operations.saturating_add(2);
    artifacts.push(artifact);
    Ok(())
}

fn directory_for(
    root: &StableDirectory,
    relative: &str,
) -> Result<(StableDirectory, String), GfError> {
    let path = Path::new(relative);
    if path.is_absolute() {
        return Err(storage("canonical artifact path is absolute"));
    }
    let mut components = path.components().collect::<Vec<_>>();
    let name = match components.pop() {
        Some(Component::Normal(name)) => name
            .to_str()
            .ok_or_else(|| storage("canonical artifact name is not UTF-8"))?
            .to_owned(),
        _ => return Err(storage("canonical artifact path has no file name")),
    };
    let mut directory = root
        .create_child_directory(OsStr::new("graph"))
        .map_err(storage)?;
    for component in components {
        let Component::Normal(name) = component else {
            return Err(storage("canonical artifact path is not normalized"));
        };
        directory = directory.create_child_directory(name).map_err(storage)?;
    }
    Ok((directory, name))
}

fn authenticate_file(path: &str, file: &mut File) -> Result<ConstructionEncodedArtifact, GfError> {
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    file.rewind().map_err(storage)?;
    loop {
        let read = file.read(&mut buffer).map_err(storage)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    Ok(ConstructionEncodedArtifact {
        path: path.to_owned(),
        bytes,
        sha256: hex(&digest.finalize()),
    })
}

fn read_inventory(root: &StableDirectory) -> Result<Option<GraphConstructionEncoding>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(INVENTORY)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    if file.metadata().map_err(storage)?.len() > MAX_INVENTORY_BYTES {
        return Err(storage("canonical inventory exceeds bound"));
    }
    serde_json::from_reader(&mut file)
        .map(Some)
        .map_err(storage)
}

fn authenticate_inventory(
    root: &StableDirectory,
    inventory: &GraphConstructionEncoding,
) -> Result<(), GfError> {
    if inventory.root != ENCODED_ROOT
        || inventory.shape_inputs_sha256.len() != 64
        || inventory
            .semantic_authority_sha256
            .as_ref()
            .is_some_and(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || !inventory
            .shape_inputs_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || inventory
            .artifacts
            .windows(2)
            .any(|pair| pair[0].path >= pair[1].path)
        || inventory.evidence.prior_topology_rows_decoded != 0
        || inventory.evidence.retained_topology_bytes_copied != 0
    {
        return Err(storage("canonical inventory invariants are invalid"));
    }
    for expected in &inventory.artifacts {
        let (directory, name) = directory_for(root, &expected.path)?;
        let mut file = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        let actual = authenticate_file(&expected.path, &mut file)?;
        if &actual != expected {
            return Err(storage("canonical artifact differs from inventory"));
        }
    }
    Ok(())
}

fn install_json<T: Serialize>(
    root: &StableDirectory,
    name: &str,
    value: &T,
) -> Result<(), GfError> {
    let temporary = format!(".{name}-{}.tmp", Uuid::new_v4().simple());
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    serde_json::to_writer(&mut file, value).map_err(storage)?;
    file.flush().map_err(storage)?;
    file.sync_all().map_err(storage)?;
    drop(file);
    root.replace_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)
}

struct FixedReader<const N: usize> {
    reader: BufReader<CountingInput<File>>,
    counter: IoCounter,
}

impl<const N: usize> FixedReader<N> {
    fn open(
        root: &StableDirectory,
        name: &str,
        _evidence: &mut GraphConstructionEncodingEvidence,
    ) -> Result<Self, GfError> {
        let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        if file.metadata().map_err(storage)?.len() % N as u64 != 0 {
            return Err(storage("fixed-width construction stream is truncated"));
        }
        let counter = IoCounter::default();
        Ok(Self {
            reader: BufReader::with_capacity(
                COPY_BUFFER_BYTES,
                CountingInput {
                    inner: file,
                    counter: counter.clone(),
                },
            ),
            counter,
        })
    }

    fn next(&mut self) -> Result<Option<[u8; N]>, GfError> {
        let mut record = [0_u8; N];
        let mut read = 0;
        while read < N {
            let amount = self.reader.read(&mut record[read..]).map_err(storage)?;
            if amount == 0 {
                if read == 0 {
                    return Ok(None);
                }
                return Err(storage("fixed-width construction stream is truncated"));
            }
            read += amount;
        }
        Ok(Some(record))
    }

    fn account(&self, evidence: &mut GraphConstructionEncodingEvidence) {
        let (bytes, operations) = self.counter.values();
        evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
        evidence.input_read_operations = evidence.input_read_operations.saturating_add(operations);
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}
