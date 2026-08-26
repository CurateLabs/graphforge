//! Publication-independent canonical encoding for sealed construction shapes.
//!
//! This layer deliberately stops before graph-object installation or `CURRENT`.
//! It converts the shaper's authenticated, UUID-ordered streams into the exact
//! ordinary storage schemas and prepares a streamed UUID-membership manifest.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Component, Path};
use std::sync::Arc;

#[cfg(test)]
use std::cell::RefCell;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, ListArray, StringArray,
    TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_filesystem::{StableDirectory, file_identity, file_link_count};
use graphforge_ir::runtime_entity_type_id;
use graphforge_ir::{CompositionBindingContext, SymbolBinding};
use graphforge_ontology::{QualifiedSymbol, SymbolKind};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_construction::{
    ArtifactReceipt, ConstructionSemanticAuthority, ConstructionShape, CountingChunkReader,
    GraphConstructionBudgets, IoCounter, open_authenticated_shape_source, shaped_output_sha256,
};
use crate::property_overlay::{
    PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT,
    PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD, PropertyRouteKind,
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
const ENCODING_INTENT: &str = "encoding-intent.json";
const MAX_INVENTORY_BYTES: u64 = 16 << 20;
const IDENTITY_WIDTH: usize = 32;
const NODE_DETAIL_WIDTH: usize = 272;
const EDGE_DETAIL_WIDTH: usize = 304;
const RESOLVED_ENDPOINT_WIDTH: usize = 32;
const COPY_BUFFER_BYTES: usize = 1 << 20;

#[cfg(test)]
type SourceSpoolHook = Box<dyn FnMut(&str)>;
#[cfg(test)]
thread_local! {
    static SOURCE_SPOOL_HOOK: RefCell<Option<SourceSpoolHook>> = RefCell::new(None);
}

#[cfg(test)]
pub(crate) fn set_source_spool_hook(hook: Option<SourceSpoolHook>) {
    SOURCE_SPOOL_HOOK.with(|slot| *slot.borrow_mut() = hook);
}

#[cfg(test)]
fn run_source_spool_hook(phase: &str) {
    SOURCE_SPOOL_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(phase);
        }
    });
}

#[cfg(not(test))]
fn run_source_spool_hook(_phase: &str) {}

struct CountingWriter {
    inner: File,
    counter: IoCounter,
}

struct CountingInput<R> {
    inner: R,
    counter: IoCounter,
}

struct EncodingTempGuard<'a> {
    directory: &'a StableDirectory,
    name: String,
    identity: graphforge_filesystem::FileIdentity,
    armed: bool,
}

impl EncodingTempGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for EncodingTempGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(file) = self.directory.open_child_file(OsStr::new(&self.name))
            && file_identity(&file).ok() == Some(self.identity)
            && file_link_count(&file).ok() == Some(1)
        {
            drop(file);
            let _ = self
                .directory
                .unlink_child_if_identity(OsStr::new(&self.name), self.identity);
            let _ = self.directory.sync();
        }
    }
}

fn authenticated_source_spool<'a>(
    source: &StableDirectory,
    outputs: &[ArtifactReceipt],
    name: &str,
    output: &'a StableDirectory,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(File, EncodingTempGuard<'a>), GfError> {
    let mut authenticated = open_authenticated_shape_source(source, outputs, name)?;
    let temporary = format!(".authenticated-source-{}.tmp", Uuid::new_v4().simple());
    let mut spool = output
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let spool_identity = file_identity(&spool).map_err(storage)?;
    let guard = EncodingTempGuard {
        directory: output,
        name: temporary,
        identity: spool_identity,
        armed: true,
    };
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    loop {
        run_source_spool_hook("before_read");
        let read = authenticated.file.read(&mut buffer).map_err(storage)?;
        run_source_spool_hook("after_read");
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        spool.write_all(&buffer[..read]).map_err(storage)?;
        bytes = bytes.saturating_add(read as u64);
        evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(read as u64);
        evidence.input_read_operations = evidence.input_read_operations.saturating_add(1);
        evidence.source_spool_write_bytes = evidence
            .source_spool_write_bytes
            .saturating_add(read as u64);
        evidence.source_spool_write_operations =
            evidence.source_spool_write_operations.saturating_add(1);
    }
    if file_identity(&authenticated.file).map_err(storage)? != authenticated.identity
        || file_link_count(&authenticated.file).map_err(storage)? != 1
        || bytes != authenticated.bytes
        || hex(&digest.finalize()) != authenticated.sha256
    {
        return Err(storage(
            "shaped source changed during authenticated spooling",
        ));
    }
    spool.flush().map_err(storage)?;
    spool.sync_all().map_err(storage)?;
    evidence.source_spool_fsync_operations =
        evidence.source_spool_fsync_operations.saturating_add(1);
    evidence.source_spool_peak_temporary_bytes =
        evidence.source_spool_peak_temporary_bytes.max(bytes);
    spool.rewind().map_err(storage)?;
    Ok((spool, guard))
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
    pub membership_read_bytes: u64,
    /// Actual block reads performed by v3 construction encoding and carry.
    pub membership_read_operations: u64,
    /// Actual block writes, including outputs superseded by carry.
    pub membership_write_operations: u64,
    /// All v3 bytes written, including superseded carry inputs.
    pub membership_total_write_bytes: u64,
    /// Largest number of decoded shaped-row readers simultaneously live.
    pub peak_open_input_readers: u64,
    /// Authenticated source bytes written to one private random-access spool.
    pub source_spool_write_bytes: u64,
    /// Physical writes used by authenticated source spools.
    pub source_spool_write_operations: u64,
    /// Reads served from authenticated source spools to Parquet consumers.
    pub source_spool_read_bytes: u64,
    /// Physical reads served from authenticated source spools.
    pub source_spool_read_operations: u64,
    /// Durability barriers completed for authenticated source spools.
    pub source_spool_fsync_operations: u64,
    /// Peak owned authenticated source spool bytes (one source at a time).
    pub source_spool_peak_temporary_bytes: u64,
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
    /// Canonical authority over the sealed shape and every authenticated source receipt.
    pub shape_authority_sha256: String,
    /// Sorted, unique canonical artifact records.
    pub artifacts: Vec<ConstructionEncodedArtifact>,
    /// Authenticated retained-parent objects required to assemble this graph.
    pub retained_artifacts: Vec<ConstructionRetainedArtifact>,
    /// Measured bounded work.
    pub evidence: GraphConstructionEncodingEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct EncodingIntent {
    generation: u64,
    parent_generation: u64,
    ontology_mode: OntologyMode,
    semantic_authority_sha256: Option<String>,
    shape_inputs_sha256: String,
    shape_authority_sha256: String,
}

pub(crate) fn inventory_authority_sha256(
    inventory: &GraphConstructionEncoding,
) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-encoding-inventory/v1\0");
    digest.update(serde_json::to_vec(inventory).map_err(storage)?);
    Ok(hex(&digest.finalize()))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn encode(
    source: &StableDirectory,
    shape: &ConstructionShape,
    generation: u64,
    ontology_mode: OntologyMode,
    parent_index: Option<&AuthenticatedUuidIndexSnapshot>,
    semantic_authority: Option<&ConstructionSemanticAuthority>,
    shape_outputs: &[ArtifactReceipt],
    shape_authority_sha256: &str,
    expected_inventory_sha256: Option<&str>,
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
    if generation != shape.parent_topology_generation.saturating_add(1) {
        return Err(storage(
            "encoded generation is not consecutive to its parent",
        ));
    }
    if cancelled() {
        return Err(storage("construction encoding cancelled"));
    }
    let output = source
        .create_child_directory(OsStr::new(ENCODED_ROOT))
        .map_err(storage)?;
    cleanup_encoding_temps(&output, budgets)?;
    if let Some(existing) = read_inventory(&output)? {
        authenticate_inventory(&output, &existing, parent_index)?;
        if existing.generation != generation
            || existing.ontology_mode != shape.ontology_mode
            || existing.semantic_authority_sha256 != shape.semantic_authority_sha256
            || existing.shape_inputs_sha256 != shape.runtime_catalog_inputs_sha256
            || existing.shape_authority_sha256 != shape_authority_sha256
        {
            return Err(storage("completed encoding belongs to another generation"));
        }
        let actual_authority = inventory_authority_sha256(&existing)?;
        match expected_inventory_sha256 {
            Some(expected) if expected == actual_authority => {
                remove_encoding_intent(&output)?;
                return Ok(existing);
            }
            Some(_) => {
                return Err(storage(
                    "completed encoding differs from checkpoint inventory authority",
                ));
            }
            None => {
                // An inventory installed before its checkpoint receipt is not
                // authoritative. Regenerate it from the sealed shape instead
                // of adopting self-described output after a crash.
                remove_encoding_control(&output, INVENTORY)?;
            }
        }
    }

    let intent = EncodingIntent {
        generation,
        parent_generation: shape.parent_topology_generation,
        ontology_mode,
        semantic_authority_sha256: shape.semantic_authority_sha256.clone(),
        shape_inputs_sha256: shape.runtime_catalog_inputs_sha256.clone(),
        shape_authority_sha256: shape_authority_sha256.to_owned(),
    };
    match read_encoding_intent(&output)? {
        Some(existing) if existing != intent => {
            return Err(storage(
                "incomplete encoding intent belongs to another authority",
            ));
        }
        Some(_) => {}
        None => install_json(&output, ENCODING_INTENT, &intent)?,
    }

    let mut evidence = GraphConstructionEncodingEvidence {
        peak_open_writers: 1,
        ..Default::default()
    };
    let mut artifacts = Vec::new();
    let label_ids = {
        let (catalog_spool, _catalog_guard) = authenticated_source_spool(
            source,
            shape_outputs,
            &shape.runtime_catalog,
            &output,
            &mut evidence,
        )?;
        let label_ids = read_runtime_label_ids(
            catalog_spool.try_clone().map_err(storage)?,
            budgets,
            &mut evidence,
        )?;
        copy_artifact(
            catalog_spool.try_clone().map_err(storage)?,
            &output,
            "topology/runtime_catalog.parquet",
            &mut artifacts,
            &mut evidence,
        )?;
        label_ids
    };
    let semantic_context = semantic_authority
        .map(ConstructionSemanticAuthority::context)
        .transpose()?;

    encode_nodes(
        source,
        &output,
        shape,
        shape_outputs,
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
        shape_outputs,
        ontology_mode,
        semantic_context.as_ref(),
        semantic_authority.map(|authority| &authority.bindings),
        budgets,
        cancelled,
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
    let generation_bytes = format!(
        "{{\"topology_generation\":{generation},\"search_generation\":{generation},\"property_generation\":{generation}}}\n"
    );
    copy_artifact(
        std::io::Cursor::new(generation_bytes.into_bytes()),
        &output,
        "topology/generation.json",
        &mut artifacts,
        &mut evidence,
    )?;

    let index = crate::uuid_membership::encode_construction_index(
        source,
        &shape.identities,
        shaped_output_sha256(shape_outputs, &shape.identities)?,
        &output,
        generation,
        shape.parent_topology_generation,
        parent_index,
        shape.node_count,
        shape.edge_count,
        cancelled,
    )?;
    evidence.membership_records = index.input_records;
    evidence.membership_read_bytes = index.read_bytes;
    evidence.membership_write_bytes = index.final_write_bytes;
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
        shape_authority_sha256: shape_authority_sha256.to_owned(),
        artifacts,
        retained_artifacts,
        evidence,
    };
    install_json(&output, INVENTORY, &completed)?;
    authenticate_inventory(&output, &completed, parent_index)?;
    remove_encoding_intent(&output)?;
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
        path: format!("topology/uuid-membership/{}", value.name),
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
fn property_projections(
    input: &RecordBatch,
    required: usize,
    indexes: &[u32],
    owner: &ResolvedOwner,
    owner_kind: SymbolKind,
    route_kind: SemanticRouteKind,
    context: Option<&CompositionBindingContext>,
    bindings: Option<&SemanticStorageBindings>,
) -> Result<Vec<(String, Vec<usize>)>, GfError> {
    let mut projections = BTreeMap::<String, Vec<usize>>::new();
    for column_index in required..input.num_columns() {
        let column = input.column(column_index);
        if !indexes.iter().any(|index| !column.is_null(*index as usize)) {
            continue;
        }
        let route = if let (Some(context), Some(bindings), Some(_owner_symbol)) =
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
                .find(|candidate| candidate.route_kind == route_kind && candidate.symbol == symbol)
                .ok_or_else(|| storage("qualified property lacks owner-bound physical route"))?;
            if physical.owner.is_none() {
                return Err(storage("qualified property binding lacks declaring owner"));
            }
            physical.route.clone()
        } else {
            owner.topology_route.clone()
        };
        projections.entry(route).or_default().push(column_index);
    }
    Ok(projections.into_iter().collect())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_nodes(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    shape_outputs: &[ArtifactReceipt],
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
    let mut identities =
        FixedReader::<IDENTITY_WIDTH>::open(source, shape_outputs, &shape.identities)?;
    let mut details = FixedReader::<NODE_DETAIL_WIDTH>::open(source, shape_outputs, details_name)?;
    let rows_per_window = budgets
        .max_batch_rows
        .min((budgets.max_batch_bytes / 128).max(1));
    loop {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let mut out_uuid = Vec::with_capacity(rows_per_window);
        let mut out_id = Vec::with_capacity(rows_per_window);
        let mut out_type = Vec::with_capacity(rows_per_window);
        let mut owners = BTreeMap::<String, ResolvedOwner>::new();
        while out_uuid.len() < rows_per_window {
            let (identity, detail) = match (next_kind(&mut identities, 0)?, details.next()?) {
                (Some(identity), Some(detail)) => (identity, detail),
                (None, None) => break,
                _ => return Err(storage("node detail and identity stream lengths differ")),
            };
            if identity[..16] != detail[..16] || identity[17] != 0 {
                return Err(storage("node detail and identity streams differ"));
            }
            let uuid: [u8; 16] = detail[..16].try_into().expect("fixed UUID");
            let label_len = usize::from(detail[16]);
            let label = std::str::from_utf8(&detail[17..17 + label_len]).map_err(storage)?;
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
            if out_uuid.len().is_multiple_of(4096) && cancelled() {
                return Err(storage("construction encoding cancelled"));
            }
        }
        if out_id.is_empty() {
            break;
        }
        let canonical = node_batch(
            &out_uuid,
            &out_id,
            &out_type,
            shape.runtime_catalog_now_micros,
        )?;
        account_batch(&canonical, budgets, evidence)?;
        let first = *out_id.first().expect("nonempty");
        let last = *out_id.last().expect("nonempty");
        let path = format!("topology/nodes/{first:020}-{last:020}.parquet");
        artifacts.push(write_parquet(output, &path, &canonical, evidence)?);
    }
    if details.next()?.is_some() || next_kind(&mut identities, 0)?.is_some() {
        return Err(storage("node streams contain unconsumed rows"));
    }
    identities.authenticate_and_account(evidence)?;
    details.authenticate_and_account(evidence)?;
    encode_node_properties(
        source,
        output,
        shape,
        shape_outputs,
        ontology_mode,
        semantic_context,
        semantic_bindings,
        budgets,
        cancelled,
        artifacts,
        evidence,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_node_properties(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    shape_outputs: &[ArtifactReceipt],
    ontology_mode: OntologyMode,
    semantic_context: Option<&CompositionBindingContext>,
    semantic_bindings: Option<&SemanticStorageBindings>,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut ordinals = BTreeMap::<String, u64>::new();
    for name in &shape.node_rows {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let (file, _spool_guard) =
            authenticated_source_spool(source, shape_outputs, name, output, evidence)?;
        let counter = IoCounter::default();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
            file,
            counter: counter.clone(),
        })
        .map_err(storage)?
        .with_batch_size(budgets.max_batch_rows)
        .build()
        .map_err(storage)?;
        evidence.peak_open_input_readers = evidence.peak_open_input_readers.max(1);
        for input in &mut reader {
            let input = input.map_err(storage)?;
            account_batch(&input, budgets, evidence)?;
            if input.num_columns() == 2 {
                continue;
            }
            let labels = required_string(&input, "label")?;
            let mut groups = BTreeMap::<String, Vec<u32>>::new();
            for row in 0..input.num_rows() {
                groups
                    .entry(labels.value(row).to_owned())
                    .or_default()
                    .push(u32::try_from(row).map_err(storage)?);
            }
            for (label, indexes) in groups {
                let runtime_route = if ontology_mode == OntologyMode::Exploratory {
                    "_untyped"
                } else {
                    label.as_str()
                };
                let owner = resolve_owner(
                    semantic_context,
                    semantic_bindings,
                    SymbolKind::Entity,
                    SemanticRouteKind::Entity,
                    &label,
                    runtime_route,
                )?;
                let projections = property_projections(
                    &input,
                    2,
                    &indexes,
                    &owner,
                    SymbolKind::Entity,
                    SemanticRouteKind::NodeProperty,
                    semantic_context,
                    semantic_bindings,
                )?;
                for (route, fields) in projections {
                    let ordinal = ordinals.entry(route.clone()).or_default();
                    let property = property_batch(
                        &input,
                        "node_uuid",
                        "graphforge.entity_type",
                        &owner.topology_route,
                        &indexes,
                        &fields,
                        PropertyRouteKind::Node,
                        &route,
                        shape.parent_topology_generation + 1,
                        *ordinal,
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
                    *ordinal = ordinal.saturating_add(1);
                }
            }
        }
        let (bytes, operations) = counter.values();
        evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
        evidence.input_read_operations = evidence.input_read_operations.saturating_add(operations);
        evidence.source_spool_read_bytes = evidence.source_spool_read_bytes.saturating_add(bytes);
        evidence.source_spool_read_operations = evidence
            .source_spool_read_operations
            .saturating_add(operations);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_edges(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    shape_outputs: &[ArtifactReceipt],
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
    let mut identities =
        FixedReader::<IDENTITY_WIDTH>::open(source, shape_outputs, &shape.identities)?;
    let mut details = FixedReader::<EDGE_DETAIL_WIDTH>::open(source, shape_outputs, details_name)?;
    let mut endpoints =
        FixedReader::<RESOLVED_ENDPOINT_WIDTH>::open(source, shape_outputs, endpoints_name)?;
    let rows_per_window = budgets
        .max_batch_rows
        .min((budgets.max_batch_bytes / 192).max(1));
    loop {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let mut out_uuid = Vec::with_capacity(rows_per_window);
        let mut out_src = Vec::with_capacity(rows_per_window);
        let mut out_dst = Vec::with_capacity(rows_per_window);
        let mut out_id = Vec::with_capacity(rows_per_window);
        let mut out_src_id = Vec::with_capacity(rows_per_window);
        let mut out_dst_id = Vec::with_capacity(rows_per_window);
        let mut groups = BTreeMap::<String, Vec<u32>>::new();
        while out_uuid.len() < rows_per_window {
            let (identity, detail) = match (next_kind(&mut identities, 1)?, details.next()?) {
                (Some(identity), Some(detail)) => (identity, detail),
                (None, None) => break,
                _ => return Err(storage("edge detail and identity stream lengths differ")),
            };
            let (Some(source_endpoint), Some(target_endpoint)) =
                (endpoints.next()?, endpoints.next()?)
            else {
                return Err(storage("edge endpoint stream ended early"));
            };
            let uuid: [u8; 16] = detail[..16].try_into().expect("fixed UUID");
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
            out_uuid.push(uuid);
            out_src.push(detail[16..32].try_into().expect("fixed UUID"));
            out_dst.push(detail[32..48].try_into().expect("fixed UUID"));
            out_id.push(u64::from_be_bytes(
                identity[24..32].try_into().expect("fixed"),
            ));
            out_src_id.push(u64::from_be_bytes(
                source_endpoint[24..32].try_into().expect("fixed"),
            ));
            out_dst_id.push(u64::from_be_bytes(
                target_endpoint[24..32].try_into().expect("fixed"),
            ));
            groups
                .entry(route.to_owned())
                .or_default()
                .push(u32::try_from(out_uuid.len() - 1).map_err(storage)?);
            if out_uuid.len().is_multiple_of(4096) && cancelled() {
                return Err(storage("construction encoding cancelled"));
            }
        }
        if out_id.is_empty() {
            break;
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
        account_batch(&canonical, budgets, evidence)?;
        for (route, output_indexes) in groups {
            let mut selected = select_rows(&canonical, &output_indexes)?;
            let runtime_route = if ontology_mode == OntologyMode::Exploratory {
                "_exploratory"
            } else {
                route.as_str()
            };
            let owner = resolve_owner(
                semantic_context,
                semantic_bindings,
                SymbolKind::Relation,
                SemanticRouteKind::Relation,
                &route,
                runtime_route,
            )?;
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
        }
    }
    if details.next()?.is_some()
        || endpoints.next()?.is_some()
        || next_kind(&mut identities, 1)?.is_some()
    {
        return Err(storage("edge streams contain unconsumed rows"));
    }
    identities.authenticate_and_account(evidence)?;
    details.authenticate_and_account(evidence)?;
    endpoints.authenticate_and_account(evidence)?;
    encode_edge_properties(
        source,
        output,
        shape,
        shape_outputs,
        ontology_mode,
        semantic_context,
        semantic_bindings,
        budgets,
        cancelled,
        artifacts,
        evidence,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_edge_properties(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    shape_outputs: &[ArtifactReceipt],
    ontology_mode: OntologyMode,
    semantic_context: Option<&CompositionBindingContext>,
    semantic_bindings: Option<&SemanticStorageBindings>,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut ordinals = BTreeMap::<String, u64>::new();
    for name in &shape.edge_rows {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let (file, _spool_guard) =
            authenticated_source_spool(source, shape_outputs, name, output, evidence)?;
        let counter = IoCounter::default();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
            file,
            counter: counter.clone(),
        })
        .map_err(storage)?
        .with_batch_size(budgets.max_batch_rows)
        .build()
        .map_err(storage)?;
        evidence.peak_open_input_readers = evidence.peak_open_input_readers.max(1);
        for input in &mut reader {
            let input = input.map_err(storage)?;
            account_batch(&input, budgets, evidence)?;
            if input.num_columns() == 4 {
                continue;
            }
            let routes = required_string(&input, "rel_type")?;
            let mut groups = BTreeMap::<String, Vec<u32>>::new();
            for row in 0..input.num_rows() {
                groups
                    .entry(routes.value(row).to_owned())
                    .or_default()
                    .push(u32::try_from(row).map_err(storage)?);
            }
            for (route, indexes) in groups {
                let runtime_route = if ontology_mode == OntologyMode::Exploratory {
                    "_exploratory"
                } else {
                    route.as_str()
                };
                let owner = resolve_owner(
                    semantic_context,
                    semantic_bindings,
                    SymbolKind::Relation,
                    SemanticRouteKind::Relation,
                    &route,
                    runtime_route,
                )?;
                let projections = property_projections(
                    &input,
                    4,
                    &indexes,
                    &owner,
                    SymbolKind::Relation,
                    SemanticRouteKind::EdgeProperty,
                    semantic_context,
                    semantic_bindings,
                )?;
                for (property_route, fields) in projections {
                    let ordinal = ordinals.entry(property_route.clone()).or_default();
                    let property = property_batch(
                        &input,
                        "edge_uuid",
                        "graphforge.rel_type",
                        &owner.topology_route,
                        &indexes,
                        &fields,
                        PropertyRouteKind::Edge,
                        &property_route,
                        shape.parent_topology_generation + 1,
                        *ordinal,
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
                    *ordinal = ordinal.saturating_add(1);
                }
            }
        }
        let (bytes, operations) = counter.values();
        evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
        evidence.input_read_operations = evidence.input_read_operations.saturating_add(operations);
        evidence.source_spool_read_bytes = evidence.source_spool_read_bytes.saturating_add(bytes);
        evidence.source_spool_read_operations = evidence
            .source_spool_read_operations
            .saturating_add(operations);
    }
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
    kind: PropertyRouteKind,
    route: &str,
    generation: u64,
    ordinal: u64,
) -> Result<RecordBatch, GfError> {
    let indexes = UInt32Array::from(indexes.to_vec());
    let mut fields = vec![
        uuid_field(uuid_name),
        Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
    ];
    fields.extend(
        field_indexes
            .iter()
            .map(|index| input.schema().field(*index).clone()),
    );
    let schema = Schema::new(fields).with_metadata(
        [
            (metadata_key.to_owned(), owner.to_owned()),
            (
                PROPERTY_OVERLAY_FORMAT_KEY.to_owned(),
                PROPERTY_OVERLAY_FORMAT.to_owned(),
            ),
            (PROPERTY_ROUTE_KEY.to_owned(), route.to_owned()),
            (
                PROPERTY_KIND_KEY.to_owned(),
                kind.metadata_value().to_owned(),
            ),
            (PROPERTY_GENERATION_KEY.to_owned(), generation.to_string()),
            (PROPERTY_ORDINAL_KEY.to_owned(), ordinal.to_string()),
        ]
        .into_iter()
        .collect(),
    );
    let mut columns = std::iter::once(input.column(0))
        .chain(field_indexes.iter().map(|index| input.column(*index)))
        .map(|array| take(array.as_ref(), &indexes, None).map_err(storage))
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    columns.insert(
        1,
        Arc::new(BooleanArray::from(vec![false; indexes.len()])) as ArrayRef,
    );
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
    file: File,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<BTreeMap<String, u32>, GfError> {
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
    evidence.source_spool_read_bytes = evidence.source_spool_read_bytes.saturating_add(bytes);
    evidence.source_spool_read_operations = evidence
        .source_spool_read_operations
        .saturating_add(operations);
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
    let mut temporary_guard = EncodingTempGuard {
        directory: &directory,
        name: temporary.clone(),
        identity,
        armed: true,
    };
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
    crate::graph_construction::construction_failpoint(&format!(
        "encode.parquet.after_temp_fsync.{relative}"
    ));
    let artifact = authenticate_file(relative, &mut file)?;
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(&name))
        .map_err(storage)?;
    temporary_guard.disarm();
    directory.sync().map_err(storage)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.parquet.after_install.{relative}"
    ));
    let (written, operations) = counter.values();
    evidence.output_write_bytes = evidence.output_write_bytes.saturating_add(written);
    evidence.output_write_operations = evidence.output_write_operations.saturating_add(operations);
    evidence.fsync_operations = evidence.fsync_operations.saturating_add(2);
    Ok(artifact)
}

fn copy_artifact<R: Read + Seek>(
    mut source_file: R,
    output: &StableDirectory,
    relative: &str,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let read_counter = IoCounter::default();
    source_file.rewind().map_err(storage)?;
    let mut input = BufReader::with_capacity(
        COPY_BUFFER_BYTES,
        CountingInput {
            inner: source_file,
            counter: read_counter.clone(),
        },
    );
    let (directory, name) = directory_for(output, relative)?;
    let temporary = format!(".{}-{}.tmp", name, Uuid::new_v4().simple());
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut temporary_guard = EncodingTempGuard {
        directory: &directory,
        name: temporary.clone(),
        identity,
        armed: true,
    };
    let write_counter = IoCounter::default();
    let mut writer = BufWriter::with_capacity(
        COPY_BUFFER_BYTES,
        CountingWriter {
            inner: file,
            counter: write_counter.clone(),
        },
    );
    let bytes = std::io::copy(&mut input, &mut writer).map_err(storage)?;
    writer.flush().map_err(storage)?;
    writer.get_ref().inner.sync_all().map_err(storage)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.copy.after_temp_fsync.{relative}"
    ));
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
    temporary_guard.disarm();
    directory.sync().map_err(storage)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.copy.after_install.{relative}"
    ));
    evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
    let (read_bytes, read_operations) = read_counter.values();
    if read_bytes != bytes {
        return Err(storage("copied canonical artifact read accounting differs"));
    }
    evidence.input_read_operations = evidence
        .input_read_operations
        .saturating_add(read_operations);
    evidence.source_spool_read_bytes = evidence.source_spool_read_bytes.saturating_add(read_bytes);
    evidence.source_spool_read_operations = evidence
        .source_spool_read_operations
        .saturating_add(read_operations);
    evidence.output_write_bytes = evidence.output_write_bytes.saturating_add(bytes);
    let (write_bytes, write_operations) = write_counter.values();
    if write_bytes != bytes {
        return Err(storage(
            "copied canonical artifact write accounting differs",
        ));
    }
    evidence.output_write_operations = evidence
        .output_write_operations
        .saturating_add(write_operations);
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

fn read_encoding_intent(root: &StableDirectory) -> Result<Option<EncodingIntent>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(ENCODING_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    if file.metadata().map_err(storage)?.len() > MAX_INVENTORY_BYTES {
        return Err(storage("encoding intent exceeds bound"));
    }
    serde_json::from_reader(&mut file)
        .map(Some)
        .map_err(storage)
}

fn remove_encoding_intent(root: &StableDirectory) -> Result<(), GfError> {
    remove_encoding_control(root, ENCODING_INTENT)
}

fn remove_encoding_control(root: &StableDirectory, name: &str) -> Result<(), GfError> {
    let file = match root.open_child_file(OsStr::new(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("encoding control has unexpected links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn cleanup_encoding_temps(
    root: &StableDirectory,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    let limit = usize::try_from(budgets.max_chunks)
        .unwrap_or(usize::MAX)
        .saturating_mul(12)
        .saturating_add(budgets.max_schema_groups.saturating_mul(8))
        .max(1024);
    let mut visited = 0_usize;
    cleanup_encoding_directory(root, limit, &mut visited)
}

fn cleanup_encoding_directory(
    directory: &StableDirectory,
    limit: usize,
    visited: &mut usize,
) -> Result<(), GfError> {
    let mut changed = false;
    for name in directory.child_names().map_err(storage)? {
        *visited = visited.saturating_add(1);
        if *visited > limit {
            return Err(storage("private encoding cleanup inventory exceeds bound"));
        }
        if let Ok(file) = directory.open_child_file(&name) {
            let text = name
                .to_str()
                .ok_or_else(|| storage("private encoding file name is not UTF-8"))?;
            if is_encoding_temp(text) {
                if file_link_count(&file).map_err(storage)? != 1 {
                    return Err(storage("private encoding temp has unexpected links"));
                }
                let identity = file_identity(&file).map_err(storage)?;
                drop(file);
                directory
                    .unlink_child_if_identity(&name, identity)
                    .map_err(storage)?;
                changed = true;
            }
            continue;
        }
        let child = directory.open_child_directory(&name).map_err(storage)?;
        cleanup_encoding_directory(&child, limit, visited)?;
    }
    if changed {
        directory.sync().map_err(storage)?;
    }
    Ok(())
}

fn is_encoding_temp(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix('.')
        .and_then(|value| value.strip_suffix(".tmp"))
    else {
        return false;
    };
    body.rsplit_once('-').is_some_and(|(_, nonce)| {
        nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn authenticate_inventory(
    root: &StableDirectory,
    inventory: &GraphConstructionEncoding,
    parent_index: Option<&AuthenticatedUuidIndexSnapshot>,
) -> Result<(), GfError> {
    if inventory.root != ENCODED_ROOT
        || inventory.shape_inputs_sha256.len() != 64
        || inventory.shape_authority_sha256.len() != 64
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
        || !inventory
            .shape_authority_sha256
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
    if inventory.retained_artifacts.is_empty() {
        if inventory.evidence.retained_index_runs != 0 {
            return Err(storage("retained-index evidence lacks references"));
        }
    } else {
        let parent = parent_index
            .ok_or_else(|| storage("retained artifacts lack authenticated parent snapshot"))?;
        let mut previous = None;
        for retained in &inventory.retained_artifacts {
            if previous.is_some_and(|value: &str| value >= retained.target_path.as_str()) {
                return Err(storage(
                    "retained artifact targets are not unique and sorted",
                ));
            }
            parent.authenticate_construction_reference(
                &retained.source_root,
                retained.source_root_volume,
                &retained.source_root_file_id,
                &retained.source_path,
                retained.source_volume,
                &retained.source_file_id,
                &retained.target_path,
                retained.bytes,
                &retained.sha256,
                &retained.parent_manifest_sha256,
            )?;
            previous = Some(retained.target_path.as_str());
        }
    }
    Ok(())
}

pub(crate) fn authenticate_for_publication(
    source: &StableDirectory,
    inventory: &GraphConstructionEncoding,
) -> Result<(), GfError> {
    let encoded = source
        .open_child_directory(OsStr::new(ENCODED_ROOT))
        .map_err(storage)?;
    let recorded = read_inventory(&encoded)?
        .ok_or_else(|| storage("canonical encoding inventory is absent"))?;
    if &recorded != inventory {
        return Err(storage(
            "publication inventory differs from durable encoding",
        ));
    }
    for expected in &inventory.artifacts {
        let (directory, name) = directory_for(&encoded, &expected.path)?;
        let mut file = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        if authenticate_file(&expected.path, &mut file)? != *expected {
            return Err(storage("canonical artifact differs from durable inventory"));
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
    let mut temporary_guard = EncodingTempGuard {
        directory: root,
        name: temporary.clone(),
        identity,
        armed: true,
    };
    serde_json::to_writer(&mut file, value).map_err(storage)?;
    file.flush().map_err(storage)?;
    file.sync_all().map_err(storage)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.control.after_temp_fsync.{name}"
    ));
    drop(file);
    root.replace_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    temporary_guard.disarm();
    root.sync().map_err(storage)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.control.after_install.{name}"
    ));
    Ok(())
}

struct FixedReader<const N: usize> {
    reader: BufReader<CountingInput<File>>,
    counter: IoCounter,
    identity: graphforge_filesystem::FileIdentity,
    expected_bytes: u64,
    expected_sha256: String,
    digest: Sha256,
    consumed_bytes: u64,
}

impl<const N: usize> FixedReader<N> {
    fn open(
        root: &StableDirectory,
        outputs: &[ArtifactReceipt],
        name: &str,
    ) -> Result<Self, GfError> {
        let authenticated = open_authenticated_shape_source(root, outputs, name)?;
        let file = authenticated.file;
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
            identity: authenticated.identity,
            expected_bytes: authenticated.bytes,
            expected_sha256: authenticated.sha256,
            digest: Sha256::new(),
            consumed_bytes: 0,
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
        self.digest.update(record);
        self.consumed_bytes = self.consumed_bytes.saturating_add(N as u64);
        Ok(Some(record))
    }

    fn authenticate_and_account(
        &self,
        evidence: &mut GraphConstructionEncodingEvidence,
    ) -> Result<(), GfError> {
        if file_identity(&self.reader.get_ref().inner).map_err(storage)? != self.identity
            || file_link_count(&self.reader.get_ref().inner).map_err(storage)? != 1
            || self.consumed_bytes != self.expected_bytes
            || hex(&self.digest.clone().finalize()) != self.expected_sha256
        {
            return Err(storage(
                "fixed-width shaped source changed during consumption",
            ));
        }
        let (bytes, operations) = self.counter.values();
        evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
        evidence.input_read_operations = evidence.input_read_operations.saturating_add(operations);
        Ok(())
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
