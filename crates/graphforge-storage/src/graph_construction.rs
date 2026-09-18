//! Private, crash-recoverable staging for one-generation graph construction.
//!
//! Construction accepts bounded canonical Arrow windows and writes immutable
//! Parquet shards plus block-encoded sorted identity/endpoint runs. Each window
//! is acknowledged by one immutable receipt; a constant-size checkpoint names
//! the next sequence. `CURRENT` is never touched by this module's staging or
//! sealing path. A generation-last publisher consumes the sealed inventory.

mod intake;
use intake::{
    ReceiptPointer, artifact_stem, property_free_schema_sha256, receipt_from_intent, receipt_name,
    uuid_column, uuid_value, validate_artifact_name, validate_intent, validate_parquet_metadata,
    validate_receipt_artifacts, validate_receipt_semantics, write_parquet_with_properties,
};
mod io_evidence;
pub(crate) use io_evidence::{
    ConstructionFileHandle, CountingChunkReader, CountingRead, IoCounter,
};
pub use io_evidence::{GraphConstructionEvidence, StorageCategoryAuthorityCommitments};
use io_evidence::{
    HashingWriter, account_cache_release, account_encoding_cache_release,
    account_fixed_read_operations, account_fixed_write_operations, account_merge_read,
    account_merge_read_bytes, account_merge_write, account_merge_write_bytes, account_probe_work,
    account_sequential_read, account_sequential_write, checked_category_remove,
    checked_evidence_sum, combine_cache_cleanup, combine_secondary_cleanup, copy_post_shape_io,
    injected_input_release_failure, merge_cache_release_evidence, open_counted_fixed_reader,
    open_fixed_reader, read_run_record, record_active_identity_install,
    record_active_identity_remove, record_category_install, record_encoded_active_artifacts,
    record_encoding_io_evidence, record_shape_artifact_install, release_counted_reader_cache,
};
pub(crate) struct AuthenticatedShapeSource {
    pub(crate) file: File,
    pub(crate) identity: FileIdentity,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

mod shape;
use shape::{
    authenticate_shaped_output, authenticate_shaped_output_identity, is_shape_artifact_name,
    persist_shape_receipt, read_completed_shape, read_completed_shape_outputs, read_fixed,
    run_record_bytes, shape_receipt_name, validate_shape_binding, validate_sorted_run,
};
pub(crate) use shape::{open_authenticated_shape_source, shaped_output_sha256};
mod encoding_publication;
use encoding_publication::recover_publication;
mod recovery;
use recovery::{
    ReadWork, authenticate_artifact, canonical_artifact_target,
    cleanup_authenticated_control_temps, cleanup_failed_shape_output, cleanup_owned_artifact_temps,
    cleanup_shape_publication, is_control_temp, receipt_for_existing,
    receipt_for_existing_with_work, recover_shape_intent, reject_existing_merge_artifacts,
    remove_owned_directory_tree, shape_publication_failure, shape_publication_io_failure,
    unlink_named, unlink_shape_artifact, unlink_writer_capability,
};
mod controls;
use controls::{
    artifact_temp, control_sha256, decode_bounded, decode_shape_intent, initial_checkpoint_format,
    install_control, is_canonical_lower_hex, is_canonical_sha256, read_bounded_limit,
    replace_checkpoint_control, replace_control, validate_checkpoint, validate_parent_phase_bytes,
    validate_sha256,
};

mod catalog;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod diagnostics;
mod partition;
use catalog::{
    CatalogSource, build_runtime_catalog, load_parent_runtime_catalog,
    load_parent_runtime_catalog_from_compact,
};
mod partition_load;
mod partition_shaping;
mod supersession;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::construction_directory::ConstructionDirectory as StableDirectory;
use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, file_identity, file_link_count};
use graphforge_ir::{CompositionBindingContext, CompositionBindingLimits, RuntimeCatalog};
use graphforge_ontology::ActivationMode;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{ChunkReader, Length};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::UuidIndexKind;
use crate::construction_detail_codec::{DetailCodec, DetailValidator};
use crate::uuid_membership::{AuthenticatedUuidIndexSnapshot, UuidConstructionSnapshotWork};

use crate::construction_record_layout::{
    BASE_IDENTITY_WIDTH, ENDPOINT_WIDTH, FORMAT_VERSION, IDENTITY_SURROGATE_OFFSET,
    RESOLVED_ENDPOINT_WIDTH, RESOLVED_SURROGATE_OFFSET,
};
const PRIVATE_ROOT: &str = ".graphforge-construction";
const SESSION_LOCK: &str = "session.lock";
const CHECKPOINT: &str = "checkpoint.json";
const INTENT: &str = "intent.json";
const SHAPE_INTENT: &str = "shape-intent.json";
const PUBLICATION_INTENT: &str = "publication-intent.json";
const PUBLICATION_RECEIPT: &str = "publication-receipt.json";
const BLOCK_BYTES: usize = 1 << 20;
// Checkpoints include bounded authenticated allocation identities. The supported
// heterogeneous-schema cardinality can legitimately exceed 64 KiB while still
// remaining far below the separately bounded shape inventory.
const MAX_CONTROL_BYTES: u64 = 1 << 20;
const MAX_SHAPE_CONTROL_BYTES: u64 = 32 << 20;
const IDENTITY_WIDTH: usize = 16;
const NODE_DETAIL_WIDTH: usize = 272;
const EDGE_DETAIL_WIDTH: usize = 304;

const fn durable_lifecycle_mode() -> crate::filesystem_admission::ProjectLifecycleMode {
    crate::filesystem_admission::ProjectLifecycleMode::Durable
}

fn resume_parent_topology_generation(
    project_dir: &Path,
    operation_uuid: Uuid,
) -> Result<u64, GfError> {
    let project = StableDirectory::open(project_dir).map_err(storage)?;
    let private = project
        .open_child_directory(OsStr::new(PRIVATE_ROOT))
        .map_err(storage)?;
    let operation = private
        .open_child_directory(OsStr::new(&operation_uuid.simple().to_string()))
        .map_err(storage)?;
    let mut checkpoint_file = operation
        .open_child_file(OsStr::new(CHECKPOINT))
        .map_err(storage)?;
    let checkpoint: Checkpoint = decode_bounded(&mut checkpoint_file)?;
    if checkpoint.operation_uuid != operation_uuid
        || !checkpoint.project_identity.matches(project.identity())
        || !checkpoint.session_identity.matches(operation.identity())
    {
        return Err(storage("construction resume identity changed"));
    }
    Ok(checkpoint.parent_topology_generation)
}

/// Storage-normalized node input. API validation resolves nullable/generated
/// identities before this boundary; trailing columns are normalized properties.
pub static CONSTRUCTION_NODE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("label", DataType::Utf8, false),
    ]))
});

/// Storage-normalized edge input. Trailing columns are normalized properties.
pub static CONSTRUCTION_EDGE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
    ]))
});

/// Fixed application buffer used by canonical construction encoding streams.
pub const GRAPH_CONSTRUCTION_ENCODING_BUFFER_BYTES: usize = 1 << 20;

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("graph construction session: {error}"))
}

fn current_parent_generation_authority(project_dir: &Path) -> Result<(Uuid, String), GfError> {
    current_parent_generation(project_dir).map(|(uuid, digest, _)| (uuid, digest))
}

fn current_parent_generation(
    project_dir: &Path,
) -> Result<(Uuid, String, Option<crate::ResolvedProjectGeneration>), GfError> {
    match crate::resolve_project_generation(project_dir) {
        Ok(parent) => Ok((
            parent.generation_uuid(),
            hex(&parent.manifest_sha256()),
            Some(parent),
        )),
        Err(error) => {
            #[cfg(test)]
            {
                let _ = &error;
                // Unit fixtures predating the project-container layer exercise
                // construction mechanics only. Give them a stable non-nil
                // synthetic authority; production never takes this branch.
                let identity =
                    file_identity(&File::open(project_dir).map_err(storage)?).map_err(storage)?;
                let mut digest = Sha256::new();
                digest.update(b"graphforge-construction-test-parent/v1\0");
                digest.update(identity.volume_serial.to_be_bytes());
                digest.update(identity.file_id);
                let bytes: [u8; 32] = digest.finalize().into();
                let mut uuid_bytes = [0_u8; 16];
                uuid_bytes.copy_from_slice(&bytes[..16]);
                uuid_bytes[0] |= 1;
                return Ok((Uuid::from_bytes(uuid_bytes), hex(&bytes), None));
            }
            #[cfg(not(test))]
            return Err(storage(format!(
                "parent project generation cannot be authenticated: {error}"
            )));
        }
    }
}

/// Authenticate the exact ordinal overlay before CAS publication. Only prior
/// descriptor paths superseded by the new manifest are removed from this generation.
fn ordinal_publication_tombstones(
    parent: &crate::ResolvedProjectGeneration,
    state: &crate::graph_object_store::GraphManifestState,
    graph: &StableDirectory,
    encoding: &GraphConstructionEncoding,
) -> Result<(Vec<String>, crate::GraphObjectIoTotals), GfError> {
    const MANIFEST: &str = "topology/uuid-membership/ordinal-v4-manifest.json";
    let mut io = crate::GraphObjectIoTotals::default();
    let prior = parent.authenticated_v4_ordinal_manifest(&mut io)?;
    let Some(expected) = encoding
        .artifacts
        .iter()
        .find(|artifact| artifact.path == MANIFEST)
    else {
        if prior.is_some() {
            return Err(storage(
                "construction append omitted selected ordinal authority",
            ));
        }
        return Ok((Vec::new(), io));
    };
    if expected.bytes > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES {
        return Err(storage("ordinal publication manifest exceeds bound"));
    }
    let index = graph
        .open_child_directory(OsStr::new("topology"))
        .and_then(|topology| topology.open_child_directory(OsStr::new("uuid-membership")))
        .map_err(storage)?;
    let mut file = index
        .open_child_file(OsStr::new("ordinal-v4-manifest.json"))
        .map_err(storage)?;
    let mut bytes = Vec::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        io.read_bytes = io
            .read_bytes
            .checked_add(count as u64)
            .ok_or_else(|| storage("ordinal read bytes overflow"))?;
        io.read_calls = io
            .read_calls
            .checked_add(1)
            .ok_or_else(|| storage("ordinal read calls overflow"))?;
        if (bytes.len() as u64)
            .checked_add(count as u64)
            .is_none_or(|size| size > expected.bytes)
        {
            return Err(storage("ordinal publication manifest changed length"));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    if bytes.len() as u64 != expected.bytes || hex(&Sha256::digest(&bytes)) != expected.sha256 {
        return Err(storage(
            "ordinal publication manifest differs from encoding receipt",
        ));
    }
    let manifest = crate::ordinal_identity_v4::decode_construction_ordinal_manifest(
        &bytes,
        encoding.generation,
    )
    .map_err(storage)?;
    let old = prior.as_ref().map_or_else(BTreeMap::new, |(_, manifest)| {
        ordinal_manifest_descriptors(manifest)
    });
    for (path, (bytes, digest)) in &old {
        if !state
            .entry(path.as_str())
            .is_some_and(|entry| entry.byte_length == *bytes && entry.content_sha256 == *digest)
        {
            return Err(storage(
                "selected ordinal artifact differs from parent inventory",
            ));
        }
    }
    let new = ordinal_manifest_descriptors(&manifest);
    for (path, (bytes, digest)) in &new {
        let owned = encoding
            .artifacts
            .iter()
            .find(|artifact| artifact.path == *path);
        if let Some(owned) = owned {
            if owned.bytes != *bytes || owned.sha256 != *digest {
                return Err(storage("encoded ordinal artifact differs from manifest"));
            }
        } else if old.get(path) != Some(&(*bytes, digest.clone())) {
            return Err(storage(
                "ordinal manifest references an unauthenticated retained artifact",
            ));
        }
    }
    Ok((
        old.into_keys()
            .filter(|path| !new.contains_key(path))
            .collect(),
        io,
    ))
}

fn ordinal_manifest_descriptors(
    manifest: &crate::V4OrdinalIdentityManifest,
) -> BTreeMap<String, (u64, String)> {
    manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .map(|artifact| {
            (
                format!("topology/uuid-membership/{}", artifact.name),
                (artifact.bytes, artifact.sha256.clone()),
            )
        })
        .collect::<BTreeMap<_, _>>()
}

fn compact_parent_inventory(
    parent: &crate::ResolvedProjectGeneration,
) -> Result<(Option<crate::GraphFilesInventory>, ReadWork), GfError> {
    let Some(crate::GraphFilesParticipant::V2(root)) = parent.declared_graph_files_participant()?
    else {
        return Ok((None, ReadWork::default()));
    };
    let mut io = crate::GraphObjectIoTotals::default();
    let (entries, _) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            crate::graph_object_store::read_graph_object_counted(
                parent.container_root(),
                digest,
                crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                &mut io,
            )
        })?;
    crate::route_component::authenticate_manifest_routes(root.format_version, &entries, |entry| {
        crate::graph_object_store::read_graph_object_counted(
            parent.container_root(),
            &entry.content_sha256,
            64 * 1024 * 1024,
            &mut io,
        )
    })?;
    let inventory = crate::graph_files::inventory_from_entries_with_version(
        entries,
        if root.format_version == crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION {
            crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
        } else {
            crate::GRAPH_FILES_RECORD_VERSION
        },
    )?;
    Ok((
        Some(inventory),
        ReadWork {
            bytes: io.read_bytes,
            operations: io.read_calls,
            ..ReadWork::default()
        },
    ))
}

fn compact_parent_surrogate_tails(
    project_dir: &Path,
    inventory: &crate::GraphFilesInventory,
) -> Result<Option<(u64, u64)>, GfError> {
    let Some(entry) = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/surrogate_tails.parquet")
    else {
        return Ok(None);
    };
    let file = crate::graph_object_store::open_graph_object_by_digest(
        project_dir,
        &entry.content_sha256,
        entry.byte_length,
    )?;
    crate::writer::read_surrogate_tails_file(file).map(Some)
}

fn authenticate_exact_parent_generation(
    project_dir: &Path,
    checkpoint: &Checkpoint,
) -> Result<(Uuid, String), GfError> {
    match crate::resolve_generation_by_uuid(project_dir, checkpoint.parent_generation_uuid) {
        Ok(parent) => {
            let authority = (parent.generation_uuid(), hex(&parent.manifest_sha256()));
            if authority.1 != checkpoint.parent_generation_manifest_sha256 {
                return Err(storage("parent generation manifest authority changed"));
            }
            Ok(authority)
        }
        Err(error) => {
            #[cfg(test)]
            {
                let synthetic = current_parent_generation_authority(project_dir)?;
                if synthetic.0 == checkpoint.parent_generation_uuid
                    && synthetic.1 == checkpoint.parent_generation_manifest_sha256
                {
                    return Ok(synthetic);
                }
            }
            Err(storage(format!(
                "exact parent project generation cannot be authenticated: {error}"
            )))
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Ordered construction phase.
#[serde(rename_all = "snake_case")]
pub enum ConstructionChunkKind {
    /// Node identities and primary types.
    Node,
    /// Edge identities, endpoints, and relation routes.
    Edge,
}

impl ConstructionChunkKind {
    const fn tag(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Fixed resource windows for staging and the later external merge.
pub struct GraphConstructionBudgets {
    /// Maximum rows in one Arrow window.
    pub max_batch_rows: usize,
    /// Maximum Arrow-owned bytes in one window.
    pub max_batch_bytes: usize,
    /// Maximum accepted chunks.
    pub max_chunks: u64,
    /// Maximum fixed-width records sorted for one chunk.
    pub max_run_records: usize,
    /// Maximum inputs opened by one external merge group.
    pub merge_fan_in: usize,
    /// Maximum exact Arrow schema groups accepted by one session. Schemas are
    /// retained as a bounded external registry and merged at encode time; they
    /// are not required to be identical across chunks.
    pub max_schema_groups: usize,
    /// Maximum trailing property columns in either stable entity schema.
    pub max_property_columns: usize,
    /// Maximum persisted runtime-catalog entries admitted from the parent.
    pub max_catalog_entries: usize,
    /// Maximum decoded Arrow bytes admitted while streaming the parent catalog.
    pub max_catalog_decoded_bytes: usize,
    /// Maximum UTF-8 identifier bytes retained by the complete runtime catalog.
    pub max_catalog_identifier_bytes: usize,
    /// Range partitions shaping cuts the identity key space into.
    ///
    /// This is a recorded format parameter, never derived from the machine. It
    /// must not be tied to `available_parallelism()` or to a thread count: the
    /// same logical input has to stage identically on hosts of different sizes.
    pub partition_count: u32,
}

impl Default for GraphConstructionBudgets {
    fn default() -> Self {
        Self {
            max_batch_rows: 65_536,
            max_batch_bytes: 64 << 20,
            max_chunks: 1_000_000,
            max_run_records: 4 * 65_536,
            merge_fan_in: 32,
            max_schema_groups: 256,
            max_property_columns: 4_096,
            max_catalog_entries: 1_000_000,
            max_catalog_decoded_bytes: 256 << 20,
            max_catalog_identifier_bytes: 64 << 20,
            partition_count: partition::DEFAULT_PARTITION_COUNT,
        }
    }
}

impl GraphConstructionBudgets {
    fn validate(self) -> Result<Self, GfError> {
        if self.max_batch_rows == 0
            || self.max_batch_bytes == 0
            || self.max_chunks == 0
            || self.max_run_records < 4 * self.max_batch_rows
            || self.merge_fan_in < 2
            || !(1..=4_096).contains(&self.max_schema_groups)
            || self.max_property_columns == 0
            || self.max_catalog_entries == 0
            || self.max_catalog_decoded_bytes == 0
            || self.max_catalog_identifier_bytes == 0
            || self.partition_count == 0
            || self.partition_count > partition::MAX_PARTITION_COUNT
        {
            return Err(storage("invalid construction budgets"));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Private session lifecycle. Sealed is not publicly committed.
#[serde(rename_all = "snake_case")]
pub enum GraphConstructionState {
    /// Accepting chunks.
    Staging,
    /// Immutable inventory ready for the generation-last publisher.
    Sealed,
    /// Explicitly abandoned.
    Aborted,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ConstructionPublicationState {
    Sealed,
    Publishing,
    Published,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Publisher input produced from a sealed construction session.
///
/// Version-nine artifact names are durable receipt references: after encoding,
/// their private payloads may be retired. Pass this handle back to the session
/// encoder for authenticated replay rather than opening the named files.
pub struct ConstructionShape {
    /// Ontology mode authenticated at session open and used for physical routing.
    pub ontology_mode: graphforge_core::OntologyMode,
    /// Digest of the exact compiled composition and physical semantic bindings.
    pub semantic_authority_sha256: Option<String>,
    /// Parent generation retained by the publisher; zero denotes an empty base.
    pub parent_topology_generation: u64,
    /// Authenticated parent UUID-manifest authority. The shaped identities file
    /// contains only this session's delta and never copies the parent payload.
    pub parent_uuid_manifest_sha256: Option<String>,
    /// UUID-sorted node/edge identity records with assigned surrogates.
    pub identities: String,
    /// UUID-sorted node type records, when nodes were staged.
    pub node_details: Option<String>,
    /// UUID-sorted edge endpoint and relation records, when edges were staged.
    pub edge_details: Option<String>,
    /// UUID-sorted normalized node row artifacts, partitioned by exact schema.
    /// Empty when every node chunk carried the bare canonical schema (#1455):
    /// the catalog is then derived from `node_details`, and this is not a
    /// count of staged nodes.
    pub node_rows: Vec<String>,
    /// UUID-sorted normalized edge row artifacts, partitioned by exact schema.
    /// Empty when every edge chunk carried the bare canonical schema (#1455).
    pub edge_rows: Vec<String>,
    /// Edge-UUID regrouped `(edge, role, node_surrogate)` endpoint run.
    pub edge_endpoints: Option<String>,
    /// Timestamp that the publisher must use for every RuntimeCatalog observation.
    pub runtime_catalog_now_micros: i64,
    /// Authority digest of the exact normalized row artifacts that feed the catalog.
    pub runtime_catalog_inputs_sha256: String,
    /// Serialized RuntimeCatalog produced once from the normalized row stream,
    /// or from the details families for property-free kinds (#1455).
    pub runtime_catalog: String,
    /// Live retained plus staged nodes.
    pub node_count: u64,
    /// Live retained plus staged edges.
    pub edge_count: u64,
    /// Assigned node surrogate tail.
    pub max_node_surrogate: u64,
    /// Assigned edge surrogate tail.
    pub max_edge_surrogate: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
/// Exact generation-pinned semantic authority for construction.
pub struct ConstructionSemanticAuthority {
    /// Generation-pinned ontology composition and declared construction mode.
    pub composition: crate::WorkspaceOntologyComposition,
    /// Compiled opaque physical routes and stable storage identifiers.
    pub bindings: crate::SemanticStorageBindings,
}

impl ConstructionSemanticAuthority {
    /// Validate the compiled composition and its stable physical bindings.
    pub fn validate(&self) -> Result<(), GfError> {
        let compiled = self.composition.compile()?;
        self.bindings.validate_against(&compiled)
    }

    fn mode(&self) -> graphforge_core::OntologyMode {
        match self.composition.profile_default {
            ActivationMode::Exploratory => graphforge_core::OntologyMode::Exploratory,
            ActivationMode::Advisory => graphforge_core::OntologyMode::Advisory,
            ActivationMode::Strict => graphforge_core::OntologyMode::Strict,
        }
    }

    pub(crate) fn digest(&self) -> Result<String, GfError> {
        let mut digest = Sha256::new();
        digest.update(b"graphforge-construction-semantic-authority/v1\0");
        digest.update(self.composition.to_canonical_json()?);
        digest.update(self.bindings.to_canonical_json()?);
        Ok(hex(&digest.finalize()))
    }

    pub(crate) fn context(&self) -> Result<CompositionBindingContext, GfError> {
        let compiled = self.composition.compile()?;
        self.bindings.validate_against(&compiled)?;
        CompositionBindingContext::new(
            Arc::new(compiled),
            self.composition.bridges.clone(),
            CompositionBindingLimits::default(),
        )
        .with_generation_storage_ids(
            self.bindings
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        )
    }
}

pub use crate::graph_construction_encoding::{
    ConstructionRetainedArtifact, GraphConstructionEncoding, GraphConstructionEncodingEvidence,
    GraphConstructionEncodingInvocationEvidence,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct IdentityRecord {
    volume_serial: u64,
    file_id: String,
}

impl From<FileIdentity> for IdentityRecord {
    fn from(value: FileIdentity) -> Self {
        Self {
            volume_serial: value.volume_serial,
            file_id: hex(&value.file_id),
        }
    }
}

impl IdentityRecord {
    fn matches(&self, value: FileIdentity) -> bool {
        self.volume_serial == value.volume_serial && self.file_id == hex(&value.file_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ArtifactReceipt {
    name: String,
    bytes: u64,
    allocated_bytes: u64,
    /// The content-addressing digest. Cryptographic, and stays cryptographic.
    sha256: String,
    /// Inline corruption checksum over the same payload, produced by the pass
    /// that wrote the bytes. Non-cryptographic by design; see
    /// [`crate::corruption_checksum`] for the two assumptions that permits.
    xxh64: String,
    identity: IdentityRecord,
    write_operations: u64,
    fsync_operations: u64,
}

#[derive(Serialize)]
struct ShapeAuthorityEnvelope<'a> {
    shape: &'a ConstructionShape,
    outputs: Vec<&'a ArtifactReceipt>,
}

pub(crate) fn shape_authority_sha256(
    shape: &ConstructionShape,
    outputs: &[ArtifactReceipt],
) -> Result<String, GfError> {
    let mut ordered = outputs.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    if ordered.windows(2).any(|pair| pair[0].name == pair[1].name) {
        return Err(storage("shape authority repeats an output receipt"));
    }
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-shape-authority/v1\0");
    digest.update(
        serde_json::to_vec(&ShapeAuthorityEnvelope {
            shape,
            outputs: ordered,
        })
        .map_err(storage)?,
    );
    Ok(hex(&digest.finalize()))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Immutable acknowledgement of one canonical Arrow chunk.
pub struct ConstructionChunkReceipt {
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    semantic_authority_sha256: Option<String>,
    prior_receipt_sha256: Option<String>,
    /// Caller-stable idempotency key.
    pub chunk_id: String,
    /// Monotonic accepted sequence.
    pub sequence: u64,
    /// Node or edge phase.
    pub kind: ConstructionChunkKind,
    /// Logical rows.
    pub rows: u64,
    /// Logical Arrow bytes charged to the window.
    pub input_bytes: u64,
    /// Canonical logical digest, independent of Arrow buffer layout.
    pub input_sha256: String,
    /// Digest of the complete normalized Arrow schema carried by the row artifact.
    pub schema_sha256: String,
    /// Fixed-width run records.
    pub run_records: u64,
    accounted_live_bytes: u64,
    parquet: ArtifactReceipt,
    identities: ArtifactReceipt,
    endpoints: Option<ArtifactReceipt>,
    details: ArtifactReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // Independent persisted facts; retirement flags preserve v6–8 wire compatibility.
struct Checkpoint {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    parent_generation_uuid: Uuid,
    parent_generation_manifest_sha256: String,
    ontology_mode: graphforge_core::OntologyMode,
    #[serde(default = "durable_lifecycle_mode")]
    lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    semantic_authority_sha256: Option<String>,
    /// One authority-bound timestamp used by every catalog/topology row produced
    /// by this operation. Reopen never consults the wall clock again.
    session_now_micros: i64,
    budgets: GraphConstructionBudgets,
    state: GraphConstructionState,
    publication_state: Option<ConstructionPublicationState>,
    next_sequence: u64,
    saw_edge: bool,
    last_receipt_sha256: Option<String>,
    has_base_snapshot: bool,
    parent_catalog_sha256: Option<String>,
    node_schema_sha256: BTreeSet<String>,
    edge_schema_sha256: BTreeSet<String>,
    #[serde(default)]
    shape_authority_sha256: Option<String>,
    #[serde(default)]
    encoding_inventory_sha256: Option<String>,
    #[serde(default)]
    inputs_retired: bool,
    #[serde(default)]
    shape_retired: bool,
    base_work: UuidConstructionSnapshotWork,
    evidence: GraphConstructionEvidence,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkIntent {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    sequence: u64,
    chunk_id: String,
    chunk_key: String,
    kind: ConstructionChunkKind,
    rows: u64,
    input_bytes: u64,
    input_sha256: String,
    schema_sha256: String,
    parent_topology_generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    semantic_authority_sha256: Option<String>,
    prior_receipt_sha256: Option<String>,
    run_records: u64,
    accounted_live_bytes: u64,
    parquet: Option<ArtifactReceipt>,
    identities: Option<ArtifactReceipt>,
    endpoints: Option<ArtifactReceipt>,
    details: Option<ArtifactReceipt>,
}

#[derive(Serialize, Deserialize)]
struct ShapeIntent {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    semantic_authority_sha256: Option<String>,
    budgets: GraphConstructionBudgets,
    last_receipt_sha256: Option<String>,
    baseline_evidence: GraphConstructionEvidence,
    final_evidence: Option<GraphConstructionEvidence>,
    complete: bool,
    shape: Option<ConstructionShape>,
    outputs: Vec<ArtifactReceipt>,
    #[serde(default)]
    shape_authority_sha256: Option<String>,
    /// Recorded range-partition splitters, canonical lower hex, strictly
    /// increasing. These are the reproducibility authority: a resumed or re-run
    /// import reuses them rather than recomputing a partition function, so the
    /// staged layout is a pure function of recorded data. Installed before any
    /// partition writes a byte.
    #[serde(default)]
    splitters: Vec<String>,
    /// Recorded range-partition splitters over the staged **node** identity
    /// domain only (#1439), canonical lower hex, strictly increasing.
    /// Endpoints, node details and node-kind rows are keyed by node UUID and
    /// are routed with these instead of `splitters`: Graph500-shaped input
    /// puts nodes and edges in disjoint UUID bands, so the joint splitters
    /// above route almost every node-keyed record into a handful of
    /// partitions. A pure function of the same recorded chunk receipts as
    /// `splitters`, so R1 holds for this set too.
    #[serde(default)]
    node_splitters: Vec<String>,
    /// Measured identity rows per effective partition, in partition order.
    #[serde(default)]
    partition_identity_rows: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConstructionPublicationIntent {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_generation_uuid: Uuid,
    parent_generation_manifest_sha256: String,
    target_generation_uuid: Uuid,
    transaction_uuid: Uuid,
    shape_authority_sha256: String,
    encoding_inventory_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConstructionPublicationReceipt {
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    intent_sha256: String,
    transaction_uuid: Uuid,
    target_generation_uuid: Uuid,
    target_generation_manifest_sha256: String,
}

static ACTIVE_OPERATIONS: LazyLock<Mutex<BTreeSet<String>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

struct ProcessReservation(String);

impl ProcessReservation {
    fn acquire(key: String) -> Result<Self, GfError> {
        let mut active = ACTIVE_OPERATIONS
            .lock()
            .map_err(|_| storage("process operation registry poisoned"))?;
        if !active.insert(key.clone()) {
            return Err(storage(
                "construction operation is already open in this process",
            ));
        }
        Ok(Self(key))
    }
}

impl Drop for ProcessReservation {
    fn drop(&mut self) {
        if let Ok(mut active) = ACTIVE_OPERATIONS.lock() {
            active.remove(&self.0);
        }
    }
}

/// Descriptor-relative, exclusively owned private construction session.
pub struct GraphConstructionSession {
    project_path: PathBuf,
    project: StableDirectory,
    root: StableDirectory,
    checkpoint: Checkpoint,
    base_snapshot: Option<AuthenticatedUuidIndexSnapshot>,
    parent_catalog: RuntimeCatalog,
    compact_parent: Option<crate::GraphFilesInventory>,
    semantic_authority: Option<ConstructionSemanticAuthority>,
    /// Whether this process has already streamed and checksummed the retained
    /// shape output payloads since it opened the session (#1392). The refusal
    /// is once per process-open: nothing inside this process mutates those
    /// bytes, and the threat model excludes an active same-identity adversary
    /// racing it (ADR 0013, recorded in `crate::corruption_checksum`).
    shape_outputs_verified: bool,
    session_lock: File,
    _reservation: ProcessReservation,
}

impl Drop for GraphConstructionSession {
    fn drop(&mut self) {
        // A concurrently forked child can retain a duplicate of this open-file
        // description until exec closes its CLOEXEC descriptors. Unlock before
        // closing our handle so session ownership ends at this Rust lifetime,
        // even while such a duplicate is still alive.
        let _ = crate::file_lock::unlock(&self.session_lock);
    }
}

impl GraphConstructionSession {
    /// Create or resume an operation pinned to one parent topology generation.
    pub fn open_with_mode(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
    ) -> Result<Self, GfError> {
        if ontology_mode != graphforge_core::OntologyMode::Exploratory {
            return Err(storage(
                "strict or advisory construction requires pinned semantic authority",
            ));
        }
        Self::open_with_mode_and_lifecycle(
            project_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            budgets,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        )
    }

    /// Open an exploratory construction under the facade's admitted lifecycle.
    pub fn open_with_mode_and_lifecycle(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        if ontology_mode != graphforge_core::OntologyMode::Exploratory {
            return Err(storage(
                "strict or advisory construction requires pinned semantic authority",
            ));
        }
        Self::open_with_mode_and_lifecycle_from_graph(
            project_dir,
            project_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            budgets,
            lifecycle_mode,
        )
    }

    /// Open using a separately materialized authenticated graph workspace.
    pub fn open_with_mode_and_lifecycle_from_graph(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        if ontology_mode != graphforge_core::OntologyMode::Exploratory {
            return Err(storage(
                "strict or advisory construction requires pinned semantic authority",
            ));
        }
        Self::open_internal(
            project_dir,
            graph_source_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            None,
            budgets,
            lifecycle_mode,
        )
    }

    /// Resume an exploratory session using its authenticated pinned parent.
    pub fn resume_with_mode_and_lifecycle(
        project_dir: &Path,
        operation_uuid: Uuid,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        Self::resume_with_mode_and_lifecycle_from_graph(
            project_dir,
            project_dir,
            operation_uuid,
            ontology_mode,
            budgets,
            lifecycle_mode,
        )
    }

    /// Resume using a separately materialized authenticated graph workspace.
    pub fn resume_with_mode_and_lifecycle_from_graph(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        let parent_topology_generation =
            resume_parent_topology_generation(project_dir, operation_uuid)?;
        Self::open_with_mode_and_lifecycle_from_graph(
            project_dir,
            graph_source_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            budgets,
            lifecycle_mode,
        )
    }

    /// Open with exact composition and physical semantic routing authority.
    pub fn open_with_semantic_authority(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        authority: ConstructionSemanticAuthority,
        budgets: GraphConstructionBudgets,
    ) -> Result<Self, GfError> {
        authority.validate()?;
        Self::open_with_semantic_authority_and_lifecycle(
            project_dir,
            operation_uuid,
            parent_topology_generation,
            authority.mode(),
            budgets,
            authority,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        )
    }

    /// Open a semantically bound construction under the admitted lifecycle.
    pub fn open_with_semantic_authority_and_lifecycle(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        authority: ConstructionSemanticAuthority,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        authority.validate()?;
        if authority.mode() != ontology_mode {
            return Err(storage("construction semantic authority mode changed"));
        }
        Self::open_with_semantic_authority_and_lifecycle_from_graph(
            project_dir,
            project_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            budgets,
            authority,
            lifecycle_mode,
        )
    }

    /// Open a semantically bound session from a materialized graph workspace.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_semantic_authority_and_lifecycle_from_graph(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        authority: ConstructionSemanticAuthority,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        authority.validate()?;
        if authority.mode() != ontology_mode {
            return Err(storage("construction semantic authority mode changed"));
        }
        Self::open_internal(
            project_dir,
            graph_source_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            Some(authority),
            budgets,
            lifecycle_mode,
        )
    }

    /// Resume a semantically bound session using its authenticated pinned parent.
    pub fn resume_with_semantic_authority_and_lifecycle(
        project_dir: &Path,
        operation_uuid: Uuid,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        authority: ConstructionSemanticAuthority,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        Self::resume_with_semantic_authority_and_lifecycle_from_graph(
            project_dir,
            project_dir,
            operation_uuid,
            ontology_mode,
            budgets,
            authority,
            lifecycle_mode,
        )
    }

    /// Resume a semantically bound session from a materialized graph workspace.
    #[allow(clippy::too_many_arguments)]
    pub fn resume_with_semantic_authority_and_lifecycle_from_graph(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        ontology_mode: graphforge_core::OntologyMode,
        budgets: GraphConstructionBudgets,
        authority: ConstructionSemanticAuthority,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        let parent_topology_generation =
            resume_parent_topology_generation(project_dir, operation_uuid)?;
        Self::open_with_semantic_authority_and_lifecycle_from_graph(
            project_dir,
            graph_source_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            budgets,
            authority,
            lifecycle_mode,
        )
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn open_internal(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        semantic_authority: Option<ConstructionSemanticAuthority>,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    ) -> Result<Self, GfError> {
        Self::open_internal_with_allocation(
            project_dir,
            graph_source_dir,
            operation_uuid,
            parent_topology_generation,
            ontology_mode,
            semantic_authority,
            budgets,
            lifecycle_mode,
            None,
        )
    }

    /// First-party diagnostic construction using the ordinary authority checks.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_allocation(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        semantic_authority: Option<ConstructionSemanticAuthority>,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
        resume: bool,
        allocation: &crate::StorageAllocationOperation,
    ) -> Result<Self, GfError> {
        if ontology_mode != graphforge_core::OntologyMode::Exploratory
            && semantic_authority.is_none()
        {
            return Err(storage(
                "strict or advisory construction requires pinned semantic authority",
            ));
        }
        let parent = if resume {
            resume_parent_topology_generation(project_dir, operation_uuid)?
        } else {
            parent_topology_generation
        };
        Self::open_internal_with_allocation(
            project_dir,
            graph_source_dir,
            operation_uuid,
            parent,
            ontology_mode,
            semantic_authority,
            budgets,
            lifecycle_mode,
            Some(allocation),
        )
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn open_internal_with_allocation(
        project_dir: &Path,
        graph_source_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        ontology_mode: graphforge_core::OntologyMode,
        semantic_authority: Option<ConstructionSemanticAuthority>,
        budgets: GraphConstructionBudgets,
        lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        let budgets = budgets.validate()?;
        let semantic_authority_sha256 = semantic_authority
            .as_ref()
            .map(ConstructionSemanticAuthority::digest)
            .transpose()?;
        let project = StableDirectory::open(project_dir)
            .map_err(storage)?
            .with_allocation(allocation.cloned());
        let project_identity = project.identity();
        let key = format!(
            "{}:{}:{}",
            project_identity.volume_serial,
            hex(&project_identity.file_id),
            operation_uuid.simple()
        );
        let reservation = ProcessReservation::acquire(key)?;
        let private = project
            .create_child_directory(OsStr::new(PRIVATE_ROOT))
            .map_err(storage)?;
        let operation_name = operation_uuid.simple().to_string();
        let root = private
            .create_child_directory(OsStr::new(&operation_name))
            .map_err(storage)?;
        // Directory handles cannot be locked on Windows. Retain an authenticated
        // regular child for the lifetime of the session and use the storage
        // layer's cross-platform lock abstraction on that exact descriptor.
        let session_lock = root
            .open_or_create_child_file(OsStr::new(SESSION_LOCK))
            .map_err(storage)?;
        if file_link_count(&session_lock).map_err(storage)? != 1 {
            return Err(storage("construction session lock has unexpected links"));
        }
        if !crate::file_lock::try_lock_exclusive(&session_lock).map_err(storage)? {
            return Err(storage(
                "construction operation is locked by another process",
            ));
        }
        let session_identity = root.identity();
        // Authenticate private recovery authority before consulting the
        // mutable public pointer. A publishing/published replay resolves its
        // exact immutable parent and therefore remains recoverable after
        // `CURRENT` has advanced.
        let mut recovered_checkpoint = match root.open_child_file(OsStr::new(CHECKPOINT)) {
            Ok(mut file) => Some(decode_bounded::<Checkpoint>(&mut file)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(storage(error)),
        };
        if let Some(checkpoint) = recovered_checkpoint.as_mut()
            && (DetailCodec::from_version(checkpoint.format_version).is_err()
                || checkpoint.operation_uuid != operation_uuid
                || !checkpoint.project_identity.matches(project_identity)
                || !checkpoint.session_identity.matches(session_identity))
        {
            return Err(storage("checkpoint private authority changed"));
        }
        if let Some(checkpoint) = recovered_checkpoint.as_mut() {
            validate_parent_phase_bytes(checkpoint)?;
            cleanup_authenticated_control_temps(
                &root,
                operation_uuid,
                project_identity,
                session_identity,
                checkpoint.format_version,
            )?;
            cleanup_owned_artifact_temps(&root)?;
            recover_publication(project_dir, &root, checkpoint)?;
        }
        let (parent_generation_uuid, parent_generation_manifest_sha256) =
            match recovered_checkpoint.as_ref() {
                Some(checkpoint)
                    if matches!(
                        checkpoint.publication_state,
                        Some(
                            ConstructionPublicationState::Publishing
                                | ConstructionPublicationState::Published
                        )
                    ) =>
                {
                    authenticate_exact_parent_generation(project_dir, checkpoint)?
                }
                Some(checkpoint) => {
                    let current = current_parent_generation_authority(project_dir)?;
                    if current.0 != checkpoint.parent_generation_uuid
                        || current.1 != checkpoint.parent_generation_manifest_sha256
                    {
                        return Err(storage("construction parent project generation changed"));
                    }
                    current
                }
                None => current_parent_generation_authority(project_dir)?,
            };
        if recovered_checkpoint.as_ref().is_none_or(|checkpoint| {
            !matches!(
                checkpoint.publication_state,
                Some(
                    ConstructionPublicationState::Publishing
                        | ConstructionPublicationState::Published
                )
            )
        }) && crate::read_topology_generation(graph_source_dir)? != parent_topology_generation
        {
            return Err(storage(
                "requested parent generation is not current at session open",
            ));
        }
        let publication_replay = recovered_checkpoint.as_ref().is_some_and(|checkpoint| {
            matches!(
                checkpoint.publication_state,
                Some(
                    ConstructionPublicationState::Publishing
                        | ConstructionPublicationState::Published
                )
            )
        });
        let (compact_inventory, compact_inventory_work) =
            if publication_replay || project_dir == graph_source_dir {
                (None, ReadWork::default())
            } else {
                let parent = crate::resolve_project_generation(project_dir)?;
                compact_parent_inventory(&parent)?
            };
        let (base_snapshot, base_work) = if publication_replay {
            (
                None,
                recovered_checkpoint
                    .as_ref()
                    .expect("publication replay has a checkpoint")
                    .base_work,
            )
        } else if parent_topology_generation == 0 {
            // A missing generation counter is not proof that a legacy parent
            // is empty. Initial construction cannot retain uncertified labels.
            for path in crate::mutator::node_parquet_files(graph_source_dir)? {
                let reader = ParquetRecordBatchReaderBuilder::try_new(
                    File::open(&path).map_err(|error| storage(error.to_string()))?,
                )
                .map_err(|error| storage(error.to_string()))?;
                if reader.metadata().file_metadata().num_rows() != 0 {
                    return Err(storage(
                        "generation-zero construction parent contains existing node rows",
                    ));
                }
            }
            (None, UuidConstructionSnapshotWork::default())
        } else {
            let mut snapshot = if let Some(inventory) = &compact_inventory {
                AuthenticatedUuidIndexSnapshot::open_from_compact_inventory(
                    project_dir,
                    inventory,
                    parent_topology_generation,
                )?
            } else {
                AuthenticatedUuidIndexSnapshot::open_at_generation(
                    graph_source_dir,
                    parent_topology_generation,
                )?
            };
            let max_node_surrogate = crate::writer::read_surrogate_tails(graph_source_dir)?
                .ok_or_else(|| storage("nonempty parent lacks surrogate tails"))?
                .0;
            let (authentication_bytes, authentication_blocks) = snapshot.take_authentication_work();
            let work = UuidConstructionSnapshotWork {
                authentication_bytes,
                authentication_blocks,
                live_nodes: snapshot.count(UuidIndexKind::Node),
                live_edges: snapshot.count(UuidIndexKind::Edge),
                max_node_surrogate,
            };
            (Some(snapshot), work)
        };
        let (parent_catalog, parent_catalog_sha256, mut parent_catalog_work) = if publication_replay
        {
            (
                RuntimeCatalog::new(),
                recovered_checkpoint
                    .as_ref()
                    .expect("publication replay has a checkpoint")
                    .parent_catalog_sha256
                    .clone(),
                ReadWork::default(),
            )
        } else if let Some(inventory) = &compact_inventory {
            load_parent_runtime_catalog_from_compact(
                project_dir,
                inventory,
                parent_topology_generation,
                budgets,
            )?
        } else {
            let graph_source = StableDirectory::open(graph_source_dir).map_err(storage)?;
            load_parent_runtime_catalog(&graph_source, parent_topology_generation, budgets)?
        };
        parent_catalog_work.bytes = parent_catalog_work
            .bytes
            .checked_add(compact_inventory_work.bytes)
            .ok_or_else(|| storage("parent inventory read bytes overflow"))?;
        parent_catalog_work.operations = parent_catalog_work
            .operations
            .checked_add(compact_inventory_work.operations)
            .ok_or_else(|| storage("parent inventory read calls overflow"))?;
        let resumed_parent_work = if recovered_checkpoint.is_some() && !publication_replay {
            ReadWork {
                bytes: base_work
                    .authentication_bytes
                    .checked_add(parent_catalog_work.bytes)
                    .ok_or_else(|| storage("resumed parent read bytes overflow"))?,
                operations: base_work
                    .authentication_blocks
                    .checked_add(parent_catalog_work.operations)
                    .ok_or_else(|| storage("resumed parent read operations overflow"))?,
                ..ReadWork::default()
            }
        } else {
            ReadWork::default()
        };
        let checkpoint = if let Some(checkpoint) = recovered_checkpoint {
            checkpoint
        } else {
            let evidence = GraphConstructionEvidence {
                seal_application_read_bytes: base_work.authentication_bytes,
                shape_application_read_bytes: parent_catalog_work.bytes,
                authentication_read_bytes: base_work.authentication_bytes,
                authentication_read_operations: base_work.authentication_blocks,
                parent_catalog_read_bytes: parent_catalog_work.bytes,
                parent_catalog_read_operations: parent_catalog_work.operations,
                peak_catalog_entries: parent_catalog.entry_count() as u64,
                peak_catalog_identifier_bytes: parent_catalog.retained_identifier_bytes() as u64,
                ..GraphConstructionEvidence::default()
            };
            let mut initial = Checkpoint {
                format_version: FORMAT_VERSION,
                operation_uuid,
                project_identity: project_identity.into(),
                session_identity: session_identity.into(),
                parent_topology_generation,
                parent_generation_uuid,
                parent_generation_manifest_sha256: parent_generation_manifest_sha256.clone(),
                ontology_mode,
                lifecycle_mode,
                semantic_authority_sha256: semantic_authority_sha256.clone(),
                session_now_micros: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(storage)?
                    .as_micros()
                    .try_into()
                    .map_err(|_| storage("session timestamp exceeds i64"))?,
                budgets,
                state: GraphConstructionState::Staging,
                publication_state: None,
                next_sequence: 0,
                saw_edge: false,
                last_receipt_sha256: None,
                has_base_snapshot: parent_topology_generation != 0,
                parent_catalog_sha256: parent_catalog_sha256.clone(),
                node_schema_sha256: BTreeSet::new(),
                edge_schema_sha256: BTreeSet::new(),
                shape_authority_sha256: None,
                encoding_inventory_sha256: None,
                inputs_retired: false,
                shape_retired: false,
                base_work,
                evidence,
            };
            initial.format_version = initial_checkpoint_format(&root, project_identity, &initial)?;
            cleanup_authenticated_control_temps(
                &root,
                operation_uuid,
                project_identity,
                session_identity,
                initial.format_version,
            )?;
            cleanup_owned_artifact_temps(&root)?;
            install_control(&root, CHECKPOINT, &initial)?;
            initial
        };
        validate_checkpoint(
            &checkpoint,
            operation_uuid,
            project_identity,
            session_identity,
            parent_topology_generation,
            ontology_mode,
            lifecycle_mode,
            semantic_authority_sha256.as_deref(),
            budgets,
            parent_catalog_sha256.as_deref(),
            parent_generation_uuid,
            &parent_generation_manifest_sha256,
        )?;
        let compact_parent = compact_inventory;
        let mut session = Self {
            project_path: project_dir.to_path_buf(),
            project,
            root,
            checkpoint,
            base_snapshot,
            parent_catalog,
            compact_parent,
            semantic_authority,
            shape_outputs_verified: false,
            session_lock,
            _reservation: reservation,
        };
        for category in crate::ArtifactCategory::ALL {
            session
                .checkpoint
                .evidence
                .storage_current
                .entry(category)
                .or_default();
            session
                .checkpoint
                .evidence
                .storage_receipt_category_authorities
                .entry(category)
                .or_default();
            session
                .checkpoint
                .evidence
                .storage_transient_peak_allocated_bytes
                .entry(category)
                .or_default();
            session
                .checkpoint
                .evidence
                .storage_receipt_transient_peak_authorities
                .entry(category)
                .or_default();
        }
        let (shape_recovery_work, shape_outputs_verified) =
            recover_shape_intent(&session.root, &mut session.checkpoint)?;
        session.shape_outputs_verified = shape_outputs_verified;
        session.recover_intent()?;
        if session
            .checkpoint
            .evidence
            .storage_allocation_transitions
            .is_empty()
            && !session
                .checkpoint
                .evidence
                .storage_active_identity_allocated_bytes
                .is_empty()
        {
            session
                .checkpoint
                .evidence
                .storage_allocation_transitions
                .push(crate::StorageAllocationTransition {
                    installed: session
                        .checkpoint
                        .evidence
                        .storage_active_identity_allocated_bytes
                        .clone(),
                    removed: BTreeSet::new(),
                });
        }
        session.revalidate_authority()?;
        session.reclaim_superseded_payloads()?;
        validate_parent_phase_bytes(&session.checkpoint)?;
        if shape_recovery_work.bytes != 0
            || shape_recovery_work.operations != 0
            || resumed_parent_work.bytes != 0
            || resumed_parent_work.operations != 0
        {
            session.checkpoint.evidence.recovery_application_read_bytes = session
                .checkpoint
                .evidence
                .recovery_application_read_bytes
                .checked_add(resumed_parent_work.bytes)
                .and_then(|value| value.checked_add(shape_recovery_work.bytes))
                .ok_or_else(|| storage("recovery read bytes overflow"))?;
            session
                .checkpoint
                .evidence
                .recovery_application_read_operations = session
                .checkpoint
                .evidence
                .recovery_application_read_operations
                .checked_add(resumed_parent_work.operations)
                .and_then(|value| value.checked_add(shape_recovery_work.operations))
                .ok_or_else(|| storage("recovery read operations overflow"))?;
            session
                .checkpoint
                .evidence
                .recovery_checkpoint_fsync_operations = session
                .checkpoint
                .evidence
                .recovery_checkpoint_fsync_operations
                .checked_add(3)
                .ok_or_else(|| storage("recovery checkpoint sync count overflow"))?;
            account_cache_release(
                shape_recovery_work.cache_release,
                &mut session.checkpoint.evidence,
            )?;
            replace_checkpoint_control(&session.root, &session.checkpoint)?;
        }
        Ok(session)
    }

    #[cfg(test)]
    fn open(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        budgets: GraphConstructionBudgets,
    ) -> Result<Self, GfError> {
        crate::open_or_initialize_project(project_dir)?;
        let materialized = project_dir.join("fixture-graph");
        let source = if materialized.is_dir() {
            materialized.as_path()
        } else {
            project_dir
        };
        Self::open_with_mode_and_lifecycle_from_graph(
            project_dir,
            source,
            operation_uuid,
            parent_topology_generation,
            graphforge_core::OntologyMode::Exploratory,
            budgets,
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        )
    }

    /// Current private state.
    #[must_use]
    pub const fn state(&self) -> GraphConstructionState {
        self.checkpoint.state
    }

    /// Whether this session crossed its sole project publication commit point.
    #[must_use]
    pub fn publication_committed(&self) -> bool {
        self.checkpoint.publication_state == Some(ConstructionPublicationState::Published)
    }

    /// Pinned parent topology generation.
    #[must_use]
    pub const fn parent_topology_generation(&self) -> u64 {
        self.checkpoint.parent_topology_generation
    }

    /// Authority-bound timestamp for deterministic topology and runtime-catalog
    /// materialization. It is created once and survives crash/reopen.
    #[must_use]
    pub const fn session_now_micros(&self) -> i64 {
        self.checkpoint.session_now_micros
    }

    /// Measured aggregate evidence.
    #[must_use]
    pub const fn evidence(&self) -> &GraphConstructionEvidence {
        &self.checkpoint.evidence
    }

    /// Number of durably accepted chunks.
    #[must_use]
    pub const fn accepted_chunks(&self) -> u64 {
        self.checkpoint.next_sequence
    }

    /// Independently reopen and authenticate every sealed artifact once, then
    /// freeze the private inventory. No generation authority changes here.
    pub fn seal(&mut self) -> Result<(), GfError> {
        self.seal_inner(true)
    }

    fn seal_inner(&mut self, authenticate_artifacts: bool) -> Result<(), GfError> {
        #[cfg(any(test, feature = "test-support"))]
        let _diagnostic_scope =
            crate::graph_construction::diagnostics::Scope::start("seal_authentication");
        self.revalidate_authority()?;
        self.recover_intent()?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("only a staging session can be sealed"));
        }
        let mut prior_digest = None;
        let mut saw_edge = false;
        let mut read_bytes = 0_u64;
        let mut read_operations = 0_u64;
        for sequence in 0..self.checkpoint.next_sequence {
            let receipt = self.read_receipt(sequence)?;
            validate_receipt_semantics(
                &receipt,
                sequence,
                self.checkpoint.budgets,
                DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
            )?;
            if receipt.kind == ConstructionChunkKind::Node && saw_edge {
                return Err(storage("node receipt follows edge receipt"));
            }
            saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
            if receipt.prior_receipt_sha256 != prior_digest {
                return Err(storage("receipt journal chain is discontinuous"));
            }
            if authenticate_artifacts {
                let work = validate_receipt_artifacts(
                    &self.root,
                    &receipt,
                    DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
                )?;
                account_cache_release(work.cache_release, &mut self.checkpoint.evidence)?;
                read_bytes = read_bytes
                    .checked_add(work.bytes)
                    .ok_or_else(|| storage("read_bytes overflows"))?;
                read_operations = read_operations
                    .checked_add(work.operations)
                    .ok_or_else(|| storage("read_operations overflows"))?;
            }
            let body = serde_json::to_vec(&receipt).map_err(storage)?;
            prior_digest = Some(sha256(&body));
        }
        if prior_digest != self.checkpoint.last_receipt_sha256 {
            return Err(storage("receipt journal tail differs from checkpoint"));
        }
        if saw_edge != self.checkpoint.saw_edge {
            return Err(storage("checkpoint phase differs from receipt journal"));
        }
        self.checkpoint.evidence.authentication_read_bytes = self
            .checkpoint
            .evidence
            .authentication_read_bytes
            .checked_add(read_bytes)
            .ok_or_else(|| storage("authentication read byte count overflows"))?;
        self.checkpoint.evidence.authentication_read_operations = self
            .checkpoint
            .evidence
            .authentication_read_operations
            .checked_add(read_operations)
            .ok_or_else(|| storage("authentication read operation count overflows"))?;
        self.checkpoint.evidence.seal_application_read_bytes = self
            .checkpoint
            .evidence
            .seal_application_read_bytes
            .checked_add(read_bytes)
            .ok_or_else(|| storage("seal application read byte count overflows"))?;
        self.checkpoint.state = GraphConstructionState::Sealed;
        self.checkpoint.publication_state = Some(ConstructionPublicationState::Sealed);
        replace_checkpoint_control(&self.root, &self.checkpoint)
    }

    /// Abort before seal. CURRENT remains unchanged.
    pub fn abort(&mut self) -> Result<(), GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("non-staging session belongs to the publisher"));
        }
        self.checkpoint.state = GraphConstructionState::Aborted;
        replace_checkpoint_control(&self.root, &self.checkpoint)
    }

    /// Authentically reclaim an unpublished session and every file it owns.
    ///
    /// Published or publishing sessions belong to generation recovery and can
    /// never be discarded through the private staging lifecycle.
    pub fn discard(mut self) -> Result<(), GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        if matches!(
            self.checkpoint.publication_state,
            Some(
                ConstructionPublicationState::Publishing | ConstructionPublicationState::Published
            )
        ) {
            return Err(storage(
                "published construction belongs to generation recovery",
            ));
        }
        self.checkpoint.state = GraphConstructionState::Aborted;
        replace_checkpoint_control(&self.root, &self.checkpoint)?;

        let private = self
            .project
            .open_child_directory(OsStr::new(PRIVATE_ROOT))
            .map_err(storage)?;
        let operation_name = self.checkpoint.operation_uuid.simple().to_string();
        let session_identity = self.root.identity();
        let mut remaining = self
            .checkpoint
            .budgets
            .max_chunks
            .checked_mul(16)
            .and_then(|limit| {
                u64::try_from(self.checkpoint.budgets.max_schema_groups)
                    .ok()
                    .and_then(|groups| limit.checked_add(groups))
            })
            .and_then(|limit| limit.checked_add(4_096))
            .ok_or_else(|| storage("construction discard traversal bound overflow"))?;
        crate::file_lock::unlock(&self.session_lock).map_err(storage)?;
        drop(self);
        remove_owned_directory_tree(
            &private,
            OsStr::new(&operation_name),
            session_identity,
            &mut remaining,
        )
    }

    fn revalidate_authority(&self) -> Result<(), GfError> {
        self.project.revalidate_named().map_err(storage)?;
        self.root.revalidate_named().map_err(storage)?;
        if !self
            .checkpoint
            .project_identity
            .matches(self.project.identity())
            || !self
                .checkpoint
                .session_identity
                .matches(self.root.identity())
        {
            return Err(storage("retained construction authority identity changed"));
        }
        if let Some(snapshot) = &self.base_snapshot {
            snapshot.revalidate()?;
        }
        Ok(())
    }
}

pub(super) fn reject_cancelled(cancelled: &mut impl FnMut() -> bool) -> Result<(), GfError> {
    if cancelled() {
        return Err(storage("construction cancelled"));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn construction_failpoint(name: &str) {
    if std::env::var("GF_CONSTRUCTION_FAILPOINT_COOKIE").as_deref()
        == Ok("graphforge-construction-test-v1")
        && std::env::var("GF_CONSTRUCTION_FAILPOINT").as_deref() == Ok(name)
    {
        std::process::exit(86);
    }
}

#[cfg(not(test))]
pub(crate) fn construction_failpoint(_name: &str) {}

fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 15) as usize] as char);
    }
    value
}

#[cfg(test)]
mod tests;
