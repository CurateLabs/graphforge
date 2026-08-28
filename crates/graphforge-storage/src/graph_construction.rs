//! Private, crash-recoverable staging for one-generation graph construction.
//!
//! Construction accepts bounded canonical Arrow windows and writes immutable
//! Parquet shards plus block-encoded sorted identity/endpoint runs. Each window
//! is acknowledged by one immutable receipt; a constant-size checkpoint names
//! the next sequence. `CURRENT` is never touched by this module's staging or
//! sealing path. A generation-last publisher consumes the sealed inventory.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, FixedSizeBinaryArray, MutableArrayData, RecordBatch, StringArray, UInt32Array,
    make_array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, StableDirectory, file_identity, file_link_count};
use graphforge_ir::{CompositionBindingContext, CompositionBindingLimits, RuntimeCatalog};
use graphforge_ontology::ActivationMode;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{ChunkReader, Length};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::UuidIndexKind;
use crate::uuid_membership::{AuthenticatedUuidIndexSnapshot, UuidConstructionSnapshotWork};

const FORMAT_VERSION: u32 = 5;
const PRIVATE_ROOT: &str = ".graphforge-construction";
const SESSION_LOCK: &str = "session.lock";
const CHECKPOINT: &str = "checkpoint.json";
const INTENT: &str = "intent.json";
const SHAPE_INTENT: &str = "shape-intent.json";
const PUBLICATION_INTENT: &str = "publication-intent.json";
const PUBLICATION_RECEIPT: &str = "publication-receipt.json";
const BLOCK_BYTES: usize = 1 << 20;
const MAX_CONTROL_BYTES: u64 = 64 << 10;
const MAX_SHAPE_CONTROL_BYTES: u64 = 32 << 20;
const IDENTITY_WIDTH: usize = 16;
const ENDPOINT_WIDTH: usize = 48;
const RESOLVED_ENDPOINT_WIDTH: usize = 32;
const NODE_DETAIL_WIDTH: usize = 272;
const EDGE_DETAIL_WIDTH: usize = 304;
const BASE_IDENTITY_WIDTH: usize = 32;

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

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("graph construction session: {error}"))
}

fn current_parent_generation_authority(project_dir: &Path) -> Result<(Uuid, String), GfError> {
    match crate::resolve_project_generation(project_dir) {
        Ok(parent) => Ok((parent.generation_uuid(), hex(&parent.manifest_sha256()))),
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
                return Ok((Uuid::from_bytes(uuid_bytes), hex(&bytes)));
            }
            #[cfg(not(test))]
            return Err(storage(format!(
                "parent project generation cannot be authenticated: {error}"
            )));
        }
    }
}

fn compact_parent_inventory(
    parent: &crate::ResolvedProjectGeneration,
) -> Result<Option<crate::GraphFilesInventory>, GfError> {
    let Some(crate::GraphFilesParticipant::V2(root)) = parent.declared_graph_files_participant()?
    else {
        return Ok(None);
    };
    let (entries, _) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            crate::read_graph_object_by_digest(parent.container_root(), digest, 64 * 1024 * 1024)
        })?;
    crate::graph_files::inventory_from_entries(entries).map(Some)
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Measured application I/O and bounded retained-window evidence.
pub struct GraphConstructionEvidence {
    /// All application-observed payload bytes read by the seal phase.
    #[serde(default)]
    pub seal_application_read_bytes: u64,
    /// All application-observed payload bytes read by canonical shaping, including
    /// authentication and merge consumption.
    #[serde(default)]
    pub shape_application_read_bytes: u64,
    /// All application-observed shaped payload bytes read by canonical encoding.
    #[serde(default)]
    pub encode_application_read_bytes: u64,
    /// Application-observed durable control bytes read immediately before publication.
    #[serde(default)]
    pub publication_application_read_bytes: u64,
    /// Application-observed source or reused-object payload bytes read by CAS adoption.
    #[serde(default)]
    pub cas_application_read_bytes: u64,
    /// Application-observed published payload bytes read while hydrating the workspace.
    #[serde(default)]
    pub hydration_application_read_bytes: u64,
    /// New canonical graph payload bytes emitted by encoding.
    #[serde(default)]
    pub canonical_output_bytes: u64,
    /// Staged artifact bytes plus structurally retained parent payload bytes.
    #[serde(default)]
    pub staged_and_retained_disk_bytes: u64,
    /// Rows accepted.
    pub input_rows: u64,
    /// Non-replay chunks accepted.
    pub input_batches: u64,
    /// Immutable Parquet shards.
    pub parquet_shards: u64,
    /// Bytes submitted through measured immutable-artifact writers.
    /// Control-record traffic is deliberately reported by the outer I/O probe.
    pub write_bytes: u64,
    /// Actual submissions by measured immutable-artifact writers.
    pub write_operations: u64,
    /// File and directory durability barriers completed for accepted artifacts.
    pub fsync_operations: u64,
    /// Bytes read for independent authentication.
    pub authentication_read_bytes: u64,
    /// Actual bounded authentication reads.
    pub authentication_read_operations: u64,
    /// Parent runtime-catalog bytes read through its retained descriptor.
    pub parent_catalog_read_bytes: u64,
    /// Parent runtime-catalog read operations observed by the bounded reader.
    pub parent_catalog_read_operations: u64,
    /// Retained UUID-index bytes loaded by bounded construction probes.
    pub retained_probe_read_bytes: u64,
    /// One-MiB retained UUID-index cache fills performed by construction probes.
    pub retained_probe_block_loads: u64,
    /// Shaped-output bytes read while constructing its authenticated inventory.
    pub shaped_output_authentication_bytes: u64,
    /// Bounded reads used to construct the shaped-output inventory.
    pub shaped_output_authentication_operations: u64,
    /// Bytes read while validating exact idempotent replays.
    pub replay_validation_read_bytes: u64,
    /// Read operations while validating exact idempotent replays.
    pub replay_validation_read_operations: u64,
    /// Bytes read while revalidating sealed inputs for canonical shaping.
    pub shape_input_validation_read_bytes: u64,
    /// Read operations while revalidating sealed inputs for canonical shaping.
    pub shape_input_validation_read_operations: u64,
    /// Fixed-width run records.
    pub run_records: u64,
    /// Largest retained Arrow row window.
    pub peak_batch_rows: u64,
    /// Largest retained Arrow byte window.
    pub peak_batch_bytes: u64,
    /// Largest fixed-width sort window.
    pub peak_run_records: u64,
    /// Prior topology rows decoded during staging. Invariant: zero.
    pub prior_topology_rows_decoded: u64,
    /// CURRENT transitions during staging. Invariant: zero.
    pub current_transitions: u64,
    /// Exact idempotent replays.
    pub replayed_chunks: u64,
    /// Temporary and final fixed-width records read by canonical shaping.
    pub merge_read_records: u64,
    /// Temporary and final fixed-width records written by canonical shaping.
    pub merge_written_records: u64,
    /// External merge groups completed (including intermediate levels).
    pub merge_groups: u64,
    /// Highest number of simultaneously open merge inputs.
    pub peak_merge_inputs: u64,
    /// Exact fixed-width payload bytes read during shaping.
    pub merge_read_bytes: u64,
    /// Exact fixed-width payload bytes written during shaping.
    pub merge_written_bytes: u64,
    /// Logical lower bound on bounded reader refills implied by payload bytes
    /// and the fixed one-MiB window; this is not an observed syscall count.
    pub merge_read_blocks: u64,
    /// Logical lower bound on bounded writer flushes implied by payload bytes
    /// and the fixed one-MiB window; this is not an observed syscall count.
    pub merge_write_blocks: u64,
    /// Number of completed merge levels across shaped domains.
    pub merge_passes: u64,
    /// Largest measured temporary merge footprint.
    pub peak_merge_temporary_bytes: u64,
    /// Largest explicitly retained application buffer set during append. This
    /// includes input Arrow buffers, extracted fixed runs, sorted Arrow output,
    /// and a conservative full-batch Parquet encoding window; allocator/RSS is
    /// intentionally measured by the outer process probe.
    pub peak_accounted_live_bytes: u64,
    /// Largest number of merge-source names retained by the online scheduler.
    pub peak_merge_name_slots: u64,
    /// Largest number of endpoint-window names retained by its online merge.
    pub peak_resolved_endpoint_name_slots: u64,
    /// Largest complete runtime-catalog entry count retained during shaping.
    pub peak_catalog_entries: u64,
    /// Largest complete runtime-catalog identifier payload retained during shaping.
    pub peak_catalog_identifier_bytes: u64,
    /// Largest decoded catalog Arrow batch retained during one streaming pass.
    pub peak_catalog_decoded_batch_bytes: u64,
    /// File and directory durability barriers completed during shaping.
    pub merge_fsync_operations: u64,
    /// Bytes returned by instrumented Parquet range/sequential reads in shaping.
    pub parquet_read_bytes: u64,
    /// Instrumented Parquet read calls in shaping.
    pub parquet_read_operations: u64,
    /// Bytes submitted by instrumented Parquet writers in shaping.
    pub parquet_write_bytes: u64,
    /// Instrumented Parquet writer submissions in shaping.
    pub parquet_write_operations: u64,
}

impl GraphConstructionEvidence {
    /// Reconciled application-observed payload/control bytes read across construction.
    #[must_use]
    pub const fn total_application_read_bytes(&self) -> u64 {
        self.seal_application_read_bytes
            .saturating_add(self.shape_application_read_bytes)
            .saturating_add(self.encode_application_read_bytes)
            .saturating_add(self.publication_application_read_bytes)
            .saturating_add(self.cas_application_read_bytes)
            .saturating_add(self.hydration_application_read_bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Publisher input produced from a sealed construction session.
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
    pub node_rows: Vec<String>,
    /// UUID-sorted normalized edge row artifacts, partitioned by exact schema.
    pub edge_rows: Vec<String>,
    /// Edge-UUID regrouped `(edge, role, node_surrogate)` endpoint run.
    pub edge_endpoints: Option<String>,
    /// Timestamp that the publisher must use for every RuntimeCatalog observation.
    pub runtime_catalog_now_micros: i64,
    /// Authority digest of the exact normalized row artifacts that feed the catalog.
    pub runtime_catalog_inputs_sha256: String,
    /// Serialized RuntimeCatalog produced once from the normalized row stream.
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
        Ok(CompositionBindingContext::new(
            Arc::new(compiled),
            self.composition.bridges.clone(),
            CompositionBindingLimits::default(),
        )
        .with_generation_storage_ids(
            self.bindings
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        ))
    }
}

pub use crate::graph_construction_encoding::{
    ConstructionRetainedArtifact, GraphConstructionEncoding, GraphConstructionEncodingEvidence,
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
    sha256: String,
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
    /// Durably bind the sealed private inventory to the one target that the
    /// existing project publisher will stage. This records replay authority;
    /// it does not install objects, stage a generation, or mutate `CURRENT`.
    #[allow(
        dead_code,
        reason = "consumed by the next #932 publisher integration slice"
    )]
    pub(crate) fn begin_publication(
        &mut self,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
    ) -> Result<ConstructionPublicationIntent, GfError> {
        self.revalidate_authority()?;
        recover_publication(&self.project_path, &self.root, &mut self.checkpoint)?;
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Publishing) {
            let intent = read_publication_intent(&self.root, &self.checkpoint)?;
            if intent.target_generation_uuid == target_generation_uuid
                && intent.transaction_uuid == transaction_uuid
            {
                return Ok(intent);
            }
            return Err(storage("publication replay target changed"));
        }
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can begin publication"));
        }
        let shape_authority_sha256 = self
            .checkpoint
            .shape_authority_sha256
            .clone()
            .ok_or_else(|| storage("publication requires completed shape authority"))?;
        let encoding_inventory_sha256 = self
            .checkpoint
            .encoding_inventory_sha256
            .clone()
            .ok_or_else(|| storage("publication requires encoded inventory authority"))?;
        let intent = ConstructionPublicationIntent {
            format_version: FORMAT_VERSION,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_generation_uuid: self.checkpoint.parent_generation_uuid,
            parent_generation_manifest_sha256: self
                .checkpoint
                .parent_generation_manifest_sha256
                .clone(),
            target_generation_uuid,
            transaction_uuid,
            shape_authority_sha256,
            encoding_inventory_sha256,
        };
        validate_publication_intent(&intent, &self.checkpoint)?;
        install_control(&self.root, PUBLICATION_INTENT, &intent)?;
        self.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
        Ok(intent)
    }

    /// Record the sole project publisher's exact durable result. Replay must
    /// supply the same target generation and manifest digest.
    #[allow(
        dead_code,
        reason = "consumed by the next #932 publisher integration slice"
    )]
    pub(crate) fn finish_publication(
        &mut self,
        target_generation_uuid: Uuid,
        target_generation_manifest_sha256: &str,
    ) -> Result<ConstructionPublicationReceipt, GfError> {
        recover_publication(&self.project_path, &self.root, &mut self.checkpoint)?;
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Published) {
            let receipt = read_publication_receipt(&self.root, &self.checkpoint)?;
            authenticate_published_target(&self.project_path, &self.checkpoint, &receipt)?;
            if receipt.target_generation_uuid == target_generation_uuid
                && receipt.target_generation_manifest_sha256 == target_generation_manifest_sha256
            {
                return Ok(receipt);
            }
            return Err(storage("published replay result changed"));
        }
        if self.checkpoint.publication_state != Some(ConstructionPublicationState::Publishing) {
            return Err(storage("publication has no durable intent"));
        }
        validate_sha256(
            target_generation_manifest_sha256,
            "target generation manifest",
        )?;
        let intent = read_publication_intent(&self.root, &self.checkpoint)?;
        if intent.target_generation_uuid != target_generation_uuid {
            return Err(storage("published target differs from durable intent"));
        }
        let provisional = ConstructionPublicationReceipt {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            intent_sha256: control_sha256(&intent)?,
            transaction_uuid: intent.transaction_uuid,
            target_generation_uuid,
            target_generation_manifest_sha256: target_generation_manifest_sha256.to_owned(),
        };
        authenticate_published_target(&self.project_path, &self.checkpoint, &provisional)?;
        let receipt = provisional;
        install_control(&self.root, PUBLICATION_RECEIPT, &receipt)?;
        self.checkpoint.publication_state = Some(ConstructionPublicationState::Published);
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
        Ok(receipt)
    }

    /// Encode a completed canonical shape into private, ordinary GraphForge
    /// graph/index artifacts. This does not publish either topology generation
    /// or project `CURRENT`; the generation-last publisher consumes the sealed
    /// inventory later.
    pub fn encode_canonical(
        &mut self,
        shape: &ConstructionShape,
        generation: u64,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.encode_canonical_with_cancellation(shape, generation, || false)
    }

    /// Cancellation-aware form of [`Self::encode_canonical`]. Installed files
    /// remain private and are deterministically regenerated on resume; only the
    /// final inventory is authoritative.
    pub fn encode_canonical_with_cancellation(
        &mut self,
        shape: &ConstructionShape,
        generation: u64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can be encoded"));
        }
        let completed = read_completed_shape(&self.root, &self.checkpoint, false)?
            .ok_or_else(|| storage("canonical shape is not complete"))?;
        if &completed != shape {
            return Err(storage("encoder input differs from completed shape"));
        }
        let shape_outputs = read_completed_shape_outputs(&self.root, &self.checkpoint)?;
        let shape_authority = shape_authority_sha256(shape, &shape_outputs)?;
        if self.checkpoint.shape_authority_sha256.as_deref() != Some(&shape_authority) {
            return Err(storage("encoder shape authority differs from checkpoint"));
        }
        let encoded = crate::graph_construction_encoding::encode(
            &self.root,
            shape,
            generation,
            self.checkpoint.ontology_mode,
            self.base_snapshot.as_ref(),
            self.semantic_authority.as_ref(),
            &shape_outputs,
            &shape_authority,
            self.checkpoint.encoding_inventory_sha256.as_deref(),
            self.checkpoint.budgets,
            &mut cancelled,
        )?;
        self.checkpoint.evidence.encode_application_read_bytes = self
            .checkpoint
            .evidence
            .encode_application_read_bytes
            .saturating_add(encoded.evidence.input_read_bytes)
            .saturating_add(encoded.evidence.membership_read_bytes);
        self.checkpoint.evidence.canonical_output_bytes = encoded
            .artifacts
            .iter()
            .map(|artifact| artifact.bytes)
            .sum();
        self.checkpoint.evidence.staged_and_retained_disk_bytes =
            self.checkpoint.evidence.write_bytes.saturating_add(
                encoded
                    .retained_artifacts
                    .iter()
                    .map(|artifact| artifact.bytes)
                    .sum(),
            );
        let inventory_authority =
            crate::graph_construction_encoding::inventory_authority_sha256(&encoded)?;
        match self.checkpoint.encoding_inventory_sha256.as_deref() {
            Some(expected) if expected != inventory_authority => {
                return Err(storage(
                    "encoded inventory differs from checkpoint authority",
                ));
            }
            Some(_) => {}
            None => {
                self.checkpoint.encoding_inventory_sha256 = Some(inventory_authority);
                replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
            }
        }
        Ok(encoded)
    }

    /// Install the authenticated canonical inventory into the project CAS and
    /// publish exactly one project generation from the session's pinned parent.
    /// The CAS lease remains held through the sole `CURRENT` replacement.
    #[allow(clippy::too_many_lines)]
    pub fn publish_canonical(
        &mut self,
        encoding: &GraphConstructionEncoding,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
    ) -> Result<crate::ProjectPublicationReceipt, GfError> {
        self.publish_canonical_with_cancellation(
            encoding,
            target_generation_uuid,
            transaction_uuid,
            || false,
        )
    }

    /// Publish while polling cancellation through the final pre-`CURRENT` boundary.
    #[allow(clippy::too_many_lines)]
    pub fn publish_canonical_with_cancellation(
        &mut self,
        encoding: &GraphConstructionEncoding,
        target_generation_uuid: Uuid,
        transaction_uuid: Uuid,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<crate::ProjectPublicationReceipt, GfError> {
        reject_cancelled(&mut cancelled)?;
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Published) {
            let published =
                crate::published_project_transaction(&self.project_path, transaction_uuid)?
                    .ok_or_else(|| storage("published construction transaction is absent"))?;
            if published.generation_uuid != target_generation_uuid {
                return Err(storage("published construction target changed"));
            }
            self.finish_publication(
                target_generation_uuid,
                &hex(&published.generation_manifest_sha256),
            )?;
            return Ok(published);
        }
        if self.checkpoint.publication_state == Some(ConstructionPublicationState::Publishing) {
            self.begin_publication(target_generation_uuid, transaction_uuid)?;
            if let Some(published) =
                crate::published_project_transaction(&self.project_path, transaction_uuid)?
            {
                if published.generation_uuid != target_generation_uuid {
                    return Err(storage("published construction target changed"));
                }
                self.finish_publication(
                    target_generation_uuid,
                    &hex(&published.generation_manifest_sha256),
                )?;
                return Ok(published);
            }
        }
        let expected_inventory = self
            .checkpoint
            .encoding_inventory_sha256
            .as_deref()
            .ok_or_else(|| storage("construction publication requires encoded inventory"))?;
        if crate::graph_construction_encoding::inventory_authority_sha256(encoding)?
            != expected_inventory
        {
            return Err(storage("publication encoding authority changed"));
        }
        if encoding.generation != self.checkpoint.parent_topology_generation.saturating_add(1) {
            return Err(storage("publication topology generation changed"));
        }
        let inventory_control_bytes =
            crate::graph_construction_encoding::authenticate_inventory_control_for_publication(
                &self.root, encoding,
            )?;
        self.checkpoint.evidence.publication_application_read_bytes = self
            .checkpoint
            .evidence
            .publication_application_read_bytes
            .saturating_add(inventory_control_bytes);
        let admission = crate::filesystem_admission::admit_project_lifecycle(
            &self.project_path,
            self.checkpoint.lifecycle_mode,
            crate::filesystem_admission::ProjectRootRequirement::Existing,
        )?;
        admission.revalidate_identity()?;
        let parent = crate::resolve_project_generation(admission.root())?;
        if parent.generation_uuid() != self.checkpoint.parent_generation_uuid
            || hex(&parent.manifest_sha256()) != self.checkpoint.parent_generation_manifest_sha256
        {
            return Err(storage("construction parent is no longer CURRENT"));
        }

        let lease = crate::begin_graph_object_publication(admission.root())?;
        let (mut manifest_state, manifest_read_bytes) =
            match parent.declared_graph_files_participant()? {
                Some(crate::GraphFilesParticipant::V2(root)) => {
                    let (state, evidence) = crate::graph_object_store::GraphManifestState::open(
                        &lease,
                        root,
                        crate::GraphManifestLimits::default(),
                    )?;
                    (state, evidence.decoded_bytes)
                }
                Some(crate::GraphFilesParticipant::V1(inventory)) if inventory.file_count == 0 => {
                    (crate::graph_object_store::GraphManifestState::empty(), 0)
                }
                Some(crate::GraphFilesParticipant::V1(_)) => {
                    return Err(storage(
                        "nonempty construction parent requires compact graph root",
                    ));
                }
                None => (crate::graph_object_store::GraphManifestState::empty(), 0),
            };
        self.checkpoint.evidence.publication_application_read_bytes = self
            .checkpoint
            .evidence
            .publication_application_read_bytes
            .saturating_add(manifest_read_bytes);
        for retained in &encoding.retained_artifacts {
            let entry = manifest_state
                .entries()
                .find(|entry| entry.relative_path == retained.target_path)
                .ok_or_else(|| storage("retained construction object is absent from parent"))?;
            if entry.byte_length != retained.bytes || entry.content_sha256 != retained.sha256 {
                return Err(storage("retained construction object authority changed"));
            }
        }

        let workspace = self
            .project_path
            .join(PRIVATE_ROOT)
            .join(self.checkpoint.operation_uuid.simple().to_string())
            .join(&encoding.root)
            .join("graph");
        let workspace_identity =
            graphforge_filesystem::path_identity(&workspace).map_err(storage)?;
        let encoded_directory = self
            .root
            .open_child_directory(OsStr::new(&encoding.root))
            .map_err(storage)?
            .open_child_directory(OsStr::new("graph"))
            .map_err(storage)?;
        if workspace_identity != encoded_directory.identity() {
            return Err(storage("encoded workspace path identity changed"));
        }
        let sealed_files = encoding
            .artifacts
            .iter()
            .map(
                |artifact| crate::graph_object_store::AuthenticatedGraphFile {
                    relative_path: PathBuf::from(&artifact.path),
                    byte_length: artifact.bytes,
                    content_sha256: artifact.sha256.clone(),
                },
            )
            .collect::<Vec<_>>();
        let (graph_root, cas_evidence) =
            crate::graph_object_store::append_authenticated_graph_files_v2(
                &lease,
                &workspace,
                &mut manifest_state,
                &sealed_files,
                &[],
            )?;
        self.checkpoint.evidence.cas_application_read_bytes = self
            .checkpoint
            .evidence
            .cas_application_read_bytes
            .saturating_add(cas_evidence.payload_bytes_hashed);
        if graphforge_filesystem::path_identity(&workspace).map_err(storage)?
            != encoded_directory.identity()
        {
            return Err(storage(
                "encoded workspace identity changed during CAS install",
            ));
        }
        for artifact in &encoding.artifacts {
            let entry = manifest_state
                .entries()
                .find(|entry| entry.relative_path == artifact.path)
                .ok_or_else(|| storage("installed construction artifact is absent"))?;
            if entry.byte_length != artifact.bytes || entry.content_sha256 != artifact.sha256 {
                return Err(storage("installed construction artifact authority changed"));
            }
        }

        // CAS installation is private and unreachable until the publication
        // intent and generation are committed. Authenticate all source bytes
        // first so corruption leaves the session sealed and retryable.
        self.begin_publication(target_generation_uuid, transaction_uuid)?;

        let graph_participant = crate::graph_files::graph_files_root_participant(&graph_root)?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect();
        let mut participants = parent
            .participant_snapshots()?
            .into_iter()
            .filter(|snapshot| {
                snapshot.capability_id != crate::GRAPH_CAPABILITY_ID
                    || snapshot.record_family_id != crate::GRAPH_FILES_FAMILY
            })
            .map(|snapshot| {
                let encoding = match snapshot.encoding.as_str() {
                    "parquet" => crate::ProjectParticipantEncoding::Parquet,
                    "arrow" => crate::ProjectParticipantEncoding::Arrow,
                    "json" => crate::ProjectParticipantEncoding::Json,
                    _ => return Err(storage("parent participant encoding is unsupported")),
                };
                Ok(crate::ProjectParticipant {
                    capability_id: snapshot.capability_id,
                    capability_version: snapshot.capability_version,
                    record_family_id: snapshot.record_family_id,
                    record_version: snapshot.record_version,
                    encoding,
                    schema_fingerprint: snapshot.schema_fingerprint,
                    row_count: snapshot.row_count,
                    bytes: snapshot.bytes,
                })
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        participants.push(graph_participant);
        let request = crate::ProjectGenerationRequest {
            transaction_uuid,
            generation_uuid: target_generation_uuid,
            capabilities,
            participants,
        };
        let publication =
            match crate::project_publication::stage_project_generation_from_admitted_parent(
                admission, parent, &request, None,
            )? {
                crate::ProjectStageOutcome::Staged(staged) => staged
                    .validate(|_| Ok(()), |_, _| Ok(()))?
                    .publish_with_graph_objects_cancellable(&lease, &mut cancelled)?,
                crate::ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            };
        construction_failpoint("publication.after_current_before_receipt");
        self.finish_publication(
            target_generation_uuid,
            &hex(&publication.generation_manifest_sha256),
        )?;
        Ok(publication)
    }

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
        let budgets = budgets.validate()?;
        let semantic_authority_sha256 = semantic_authority
            .as_ref()
            .map(ConstructionSemanticAuthority::digest)
            .transpose()?;
        let project = StableDirectory::open(project_dir).map_err(storage)?;
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
        cleanup_authenticated_control_temps(
            &root,
            operation_uuid,
            project_identity,
            session_identity,
        )?;
        cleanup_owned_artifact_temps(&root)?;
        // Authenticate private recovery authority before consulting the
        // mutable public pointer. A publishing/published replay resolves its
        // exact immutable parent and therefore remains recoverable after
        // `CURRENT` has advanced.
        let mut recovered_checkpoint = match root.open_child_file(OsStr::new(CHECKPOINT)) {
            Ok(mut file) => Some(decode_bounded::<Checkpoint>(&mut file)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(storage(error)),
        };
        if let Some(checkpoint) = recovered_checkpoint.as_mut() {
            if checkpoint.format_version != FORMAT_VERSION
                || checkpoint.operation_uuid != operation_uuid
                || !checkpoint.project_identity.matches(project_identity)
                || !checkpoint.session_identity.matches(session_identity)
            {
                return Err(storage("checkpoint private authority changed"));
            }
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
        let compact_inventory = if publication_replay || project_dir == graph_source_dir {
            None
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
        let (parent_catalog, parent_catalog_sha256, parent_catalog_work) = if publication_replay {
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
        let checkpoint = if let Some(checkpoint) = recovered_checkpoint {
            checkpoint
        } else {
            let evidence = GraphConstructionEvidence {
                authentication_read_bytes: base_work.authentication_bytes,
                authentication_read_operations: base_work.authentication_blocks,
                parent_catalog_read_bytes: parent_catalog_work.bytes,
                parent_catalog_read_operations: parent_catalog_work.operations,
                peak_catalog_entries: parent_catalog.entry_count() as u64,
                peak_catalog_identifier_bytes: parent_catalog.retained_identifier_bytes() as u64,
                ..GraphConstructionEvidence::default()
            };
            let initial = Checkpoint {
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
                base_work,
                evidence,
            };
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
            session_lock,
            _reservation: reservation,
        };
        recover_shape_intent(&session.root, &mut session.checkpoint)?;
        session.recover_intent()?;
        session.revalidate_authority()?;
        Ok(session)
    }

    /// Reopen or create the canonical encoded inventory for a sealed session.
    pub fn prepare_canonical_encoding(
        &mut self,
        generation: u64,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.prepare_canonical_encoding_with_cancellation(generation, || false)
    }

    /// Reopen or create canonical encoding while polling cooperative cancellation.
    pub fn prepare_canonical_encoding_with_cancellation(
        &mut self,
        generation: u64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        reject_cancelled(&mut cancelled)?;
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed {
            return Err(storage("only a sealed session can be prepared"));
        }
        if self.checkpoint.encoding_inventory_sha256.is_some() {
            let encoded = self
                .root
                .open_child_directory(OsStr::new("encoded-v1"))
                .map_err(storage)?;
            let inventory = crate::graph_construction_encoding::read_inventory(&encoded)?
                .ok_or_else(|| storage("encoded inventory is absent"))?;
            if inventory.generation != generation {
                return Err(storage("encoded inventory generation changed"));
            }
            return Ok(inventory);
        }
        let shape = self.shape_canonical_inner(&mut cancelled, false)?;
        self.encode_canonical_with_cancellation(&shape, generation, cancelled)
    }

    /// Seal receipt authority and immediately prepare canonical encoding with
    /// one input-authentication pass. If the process stops after the sealed
    /// checkpoint, resume performs that authentication before consuming data.
    #[doc(hidden)]
    pub fn seal_and_prepare_canonical_encoding_with_cancellation(
        &mut self,
        generation: u64,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<GraphConstructionEncoding, GfError> {
        self.seal_inner(false)?;
        let shape = self.shape_canonical_inner(&mut cancelled, false)?;
        self.encode_canonical_with_cancellation(&shape, generation, cancelled)
    }

    #[cfg(test)]
    fn open(
        project_dir: &Path,
        operation_uuid: Uuid,
        parent_topology_generation: u64,
        budgets: GraphConstructionBudgets,
    ) -> Result<Self, GfError> {
        Self::open_with_mode(
            project_dir,
            operation_uuid,
            parent_topology_generation,
            graphforge_core::OntologyMode::Exploratory,
            budgets,
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

    /// Record application-observed reads performed by the facade's post-publication
    /// hydration before the refreshed workspace becomes visible.
    #[doc(hidden)]
    pub fn record_hydration_application_read_bytes(&mut self, bytes: u64) -> Result<(), GfError> {
        self.checkpoint.evidence.hydration_application_read_bytes = self
            .checkpoint
            .evidence
            .hydration_application_read_bytes
            .saturating_add(bytes);
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
    }

    /// Number of durably accepted chunks.
    #[must_use]
    pub const fn accepted_chunks(&self) -> u64 {
        self.checkpoint.next_sequence
    }

    /// Append one canonical bounded Arrow chunk.
    pub fn append(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append_with_cancellation(kind, chunk_id, batch, || false)
    }

    /// Append while polling a caller-owned cancellation signal at durable
    /// artifact boundaries. Cancellation leaves an intent that the next open
    /// authenticates and rolls back without changing public authority.
    #[allow(clippy::too_many_lines)]
    pub fn append_with_cancellation(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        reject_cancelled(&mut cancelled)?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("session is not accepting chunks"));
        }
        validate_chunk_id(chunk_id)?;
        validate_schema(kind, batch)?;
        if batch.num_rows() == 0 {
            return Err(storage("empty construction chunk"));
        }
        let input_bytes = batch.get_array_memory_size();
        let required_columns = if kind == ConstructionChunkKind::Node {
            2
        } else {
            4
        };
        if batch.num_columns().saturating_sub(required_columns)
            > self.checkpoint.budgets.max_property_columns
        {
            return Err(storage("construction property-column budget exhausted"));
        }
        if batch.num_rows() > self.checkpoint.budgets.max_batch_rows
            || input_bytes > self.checkpoint.budgets.max_batch_bytes
            || self.checkpoint.next_sequence >= self.checkpoint.budgets.max_chunks
        {
            return Err(storage("construction resource window exhausted"));
        }
        if kind == ConstructionChunkKind::Node && self.checkpoint.saw_edge {
            return Err(storage("node chunk cannot follow edge staging"));
        }
        let input_sha256 = logical_batch_digest(kind, batch)?;
        let schema_sha256 = normalized_schema_digest(batch.schema().as_ref());
        let key_name = chunk_key_name(chunk_id);
        if let Ok(mut key_file) = self.root.open_child_file(OsStr::new(&key_name)) {
            let pointer: ReceiptPointer = decode_bounded(&mut key_file)?;
            let receipt = self.read_receipt(pointer.sequence)?;
            let receipt_body = serde_json::to_vec(&receipt).map_err(storage)?;
            if pointer.operation_uuid == self.checkpoint.operation_uuid
                && pointer.project_identity == self.checkpoint.project_identity
                && pointer.session_identity == self.checkpoint.session_identity
                && pointer.receipt_sha256 == sha256(&receipt_body)
                && receipt.chunk_id == chunk_id
                && receipt.kind == kind
                && receipt.rows == batch.num_rows() as u64
                && receipt.input_sha256 == input_sha256
                && receipt.schema_sha256 == schema_sha256
            {
                let work = validate_receipt_artifacts(&self.root, &receipt)?;
                self.checkpoint.evidence.replay_validation_read_bytes = self
                    .checkpoint
                    .evidence
                    .replay_validation_read_bytes
                    .saturating_add(work.bytes);
                self.checkpoint.evidence.replay_validation_read_operations = self
                    .checkpoint
                    .evidence
                    .replay_validation_read_operations
                    .saturating_add(work.operations);
                self.checkpoint.evidence.replayed_chunks =
                    self.checkpoint.evidence.replayed_chunks.saturating_add(1);
                replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
                return Ok(receipt);
            }
            return Err(storage("conflicting construction chunk replay"));
        }
        let (known_schemas, other_schemas) = match kind {
            ConstructionChunkKind::Node => (
                &self.checkpoint.node_schema_sha256,
                &self.checkpoint.edge_schema_sha256,
            ),
            ConstructionChunkKind::Edge => (
                &self.checkpoint.edge_schema_sha256,
                &self.checkpoint.node_schema_sha256,
            ),
        };
        if !known_schemas.contains(&schema_sha256)
            && known_schemas
                .len()
                .saturating_add(other_schemas.len())
                .saturating_add(1)
                > self.checkpoint.budgets.max_schema_groups
        {
            return Err(storage("construction schema-group budget exhausted"));
        }
        let arrays = extract_runs(kind, batch)?;
        let run_records = arrays
            .identities
            .len()
            .saturating_add(arrays.endpoints.len())
            .saturating_add(batch.num_rows());
        if run_records > self.checkpoint.budgets.max_run_records {
            return Err(storage("construction run window exhausted"));
        }
        let sequence = self.checkpoint.next_sequence;
        let mut intent = ChunkIntent {
            format_version: FORMAT_VERSION,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            chunk_id: chunk_id.to_owned(),
            chunk_key: key_name,
            kind,
            rows: batch.num_rows() as u64,
            input_bytes: input_bytes as u64,
            input_sha256,
            schema_sha256,
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            prior_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            run_records: run_records as u64,
            accounted_live_bytes: 0,
            parquet: None,
            identities: None,
            endpoints: None,
            details: None,
        };
        install_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        let stem = artifact_stem(sequence, kind);
        let sorted_batch = uuid_sorted_batch(kind, batch)?;
        let fixed_bytes = arrays
            .identities
            .len()
            .saturating_mul(IDENTITY_WIDTH)
            .saturating_add(arrays.endpoints.len().saturating_mul(ENDPOINT_WIDTH))
            .saturating_add(match &arrays.details {
                DetailRuns::Node(records) => records.len().saturating_mul(NODE_DETAIL_WIDTH),
                DetailRuns::Edge(records) => records.len().saturating_mul(EDGE_DETAIL_WIDTH),
            });
        let sorted_bytes = sorted_batch.get_array_memory_size();
        intent.accounted_live_bytes = input_bytes
            .saturating_add(fixed_bytes)
            .saturating_add(sorted_bytes.saturating_mul(2))
            .saturating_add(BLOCK_BYTES.saturating_mul(2))
            as u64;
        replace_control(&self.root, INTENT, &intent)?;
        intent.parquet = Some(write_parquet(
            &self.root,
            &format!("{stem}.parquet"),
            &sorted_batch,
        )?);
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        intent.identities = Some(write_fixed_run(
            &self.root,
            &format!("{stem}.identities.run"),
            &arrays.identities,
        )?);
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        if !arrays.endpoints.is_empty() {
            intent.endpoints = Some(write_fixed_run(
                &self.root,
                &format!("{stem}.endpoints.run"),
                &arrays.endpoints,
            )?);
            replace_control(&self.root, INTENT, &intent)?;
            reject_cancelled(&mut cancelled)?;
        }
        intent.details = Some(match &arrays.details {
            DetailRuns::Node(records) => {
                write_fixed_run(&self.root, &format!("{stem}.node-details.run"), records)?
            }
            DetailRuns::Edge(records) => {
                write_fixed_run(&self.root, &format!("{stem}.edge-details.run"), records)?
            }
        });
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        let receipt = receipt_from_intent(&intent)?;
        let receipt_name = receipt_name(sequence);
        install_control(&self.root, &receipt_name, &receipt)?;
        let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage)?;
        let pointer = ReceiptPointer {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            receipt_sha256: sha256(&receipt_bytes),
        };
        install_control(&self.root, &intent.chunk_key, &pointer)?;
        self.advance_checkpoint(&receipt, &receipt_bytes)?;
        unlink_named(&self.root, INTENT)?;
        Ok(receipt)
    }

    /// Independently reopen and authenticate every sealed artifact once, then
    /// freeze the private inventory. No generation authority changes here.
    pub fn seal(&mut self) -> Result<(), GfError> {
        self.seal_inner(true)
    }

    fn seal_inner(&mut self, authenticate_artifacts: bool) -> Result<(), GfError> {
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
            validate_receipt_semantics(&receipt, sequence, self.checkpoint.budgets)?;
            if receipt.kind == ConstructionChunkKind::Node && saw_edge {
                return Err(storage("node receipt follows edge receipt"));
            }
            saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
            if receipt.prior_receipt_sha256 != prior_digest {
                return Err(storage("receipt journal chain is discontinuous"));
            }
            if authenticate_artifacts {
                let work = validate_receipt_artifacts(&self.root, &receipt)?;
                read_bytes = read_bytes.saturating_add(work.bytes);
                read_operations = read_operations.saturating_add(work.operations);
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
            .saturating_add(read_bytes);
        self.checkpoint.evidence.authentication_read_operations = self
            .checkpoint
            .evidence
            .authentication_read_operations
            .saturating_add(read_operations);
        self.checkpoint.evidence.seal_application_read_bytes = self
            .checkpoint
            .evidence
            .seal_application_read_bytes
            .saturating_add(read_bytes);
        self.checkpoint.state = GraphConstructionState::Sealed;
        self.checkpoint.publication_state = Some(ConstructionPublicationState::Sealed);
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
    }

    /// Validate the sealed identity domains and produce deterministic,
    /// UUID-sorted canonical construction runs.  This is deliberately still
    /// private staging: the generation-last publisher owns Parquet and CURRENT.
    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    pub fn shape_canonical_with_cancellation(
        &mut self,
        cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionShape, GfError> {
        self.shape_canonical_inner(cancelled, true)
    }

    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    fn shape_canonical_inner(
        &mut self,
        mut cancelled: impl FnMut() -> bool,
        authenticate_completed_outputs: bool,
    ) -> Result<ConstructionShape, GfError> {
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can be shaped"));
        }
        reject_cancelled(&mut cancelled)?;
        if let Some(shape) =
            read_completed_shape(&self.root, &self.checkpoint, authenticate_completed_outputs)?
        {
            return Ok(shape);
        }
        reject_existing_merge_artifacts(&self.root)?;

        let fan_in = self.checkpoint.budgets.merge_fan_in;
        let mut unified = FixedMergeAccumulator::new("merge-identities", fan_in, true);
        let mut node_details = FixedMergeAccumulator::new("merge-node-details", fan_in, true);
        let mut edge_details = FixedMergeAccumulator::new("merge-edge-details", fan_in, true);
        let mut endpoints = FixedMergeAccumulator::new("merge-endpoints", fan_in, false);
        let mut row_groups: BTreeMap<(u8, String), RowMergeAccumulator> = BTreeMap::new();
        let mut catalog_authority = Sha256::new();
        let shape_intent = ShapeIntent {
            format_version: FORMAT_VERSION,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            budgets: self.checkpoint.budgets,
            last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            baseline_evidence: self.checkpoint.evidence.clone(),
            final_evidence: None,
            complete: false,
            shape: None,
            outputs: Vec::new(),
            shape_authority_sha256: None,
        };
        install_control(&self.root, SHAPE_INTENT, &shape_intent)?;
        for sequence in 0..self.checkpoint.next_sequence {
            reject_cancelled(&mut cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            // Fixed-width inputs authenticate their exact inode, length and
            // digest in the merge consumers below. Parquet's range-oriented
            // decoder cannot establish a whole-file digest, so retain exactly
            // one explicit whole-file authentication pass for that artifact.
            let mut work = authenticate_artifact(&self.root, &receipt.parquet)?;
            let metadata_work = validate_parquet_metadata(&self.root, &receipt)?;
            work.bytes = work.bytes.saturating_add(metadata_work.bytes);
            work.operations = work.operations.saturating_add(metadata_work.operations);
            self.checkpoint.evidence.shape_input_validation_read_bytes = self
                .checkpoint
                .evidence
                .shape_input_validation_read_bytes
                .saturating_add(work.bytes);
            self.checkpoint
                .evidence
                .shape_input_validation_read_operations = self
                .checkpoint
                .evidence
                .shape_input_validation_read_operations
                .saturating_add(work.operations);
            let kind = u8::from(receipt.kind == ConstructionChunkKind::Edge);
            catalog_authority.update([kind]);
            catalog_authority.update(receipt.schema_sha256.as_bytes());
            catalog_authority.update(receipt.parquet.sha256.as_bytes());
            row_groups
                .entry((kind, receipt.schema_sha256.clone()))
                .or_insert_with(|| {
                    RowMergeAccumulator::new(fan_in, &format!("{kind}-{}", receipt.schema_sha256))
                })
                .push(
                    &self.root,
                    receipt.parquet.name.clone(),
                    self.checkpoint.budgets.max_batch_rows,
                    self.checkpoint.budgets.max_batch_bytes,
                    &mut cancelled,
                    &mut self.checkpoint.evidence,
                )?;
            let name = format!("merge-unified-{sequence:020}.run");
            convert_identity_run(&self.root, &receipt, &name, &mut self.checkpoint.evidence)?;
            unified.push::<BASE_IDENTITY_WIDTH>(
                &self.root,
                name,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            match receipt.kind {
                ConstructionChunkKind::Node => {
                    let name = format!("merge-node-source-{sequence:020}.run");
                    copy_authenticated_run::<NODE_DETAIL_WIDTH>(
                        &self.root,
                        &receipt.details,
                        &name,
                        &mut self.checkpoint.evidence,
                    )?;
                    node_details.push::<NODE_DETAIL_WIDTH>(
                        &self.root,
                        name,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                }
                ConstructionChunkKind::Edge => {
                    let detail = format!("merge-edge-source-{sequence:020}.run");
                    copy_authenticated_run::<EDGE_DETAIL_WIDTH>(
                        &self.root,
                        &receipt.details,
                        &detail,
                        &mut self.checkpoint.evidence,
                    )?;
                    edge_details.push::<EDGE_DETAIL_WIDTH>(
                        &self.root,
                        detail,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                    let endpoint = format!("merge-endpoint-source-{sequence:020}.run");
                    copy_authenticated_run::<ENDPOINT_WIDTH>(
                        &self.root,
                        receipt
                            .endpoints
                            .as_ref()
                            .ok_or_else(|| storage("edge receipt lacks endpoint run"))?,
                        &endpoint,
                        &mut self.checkpoint.evidence,
                    )?;
                    endpoints.push::<ENDPOINT_WIDTH>(
                        &self.root,
                        endpoint,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                }
            }
            let retained_names = unified
                .slot_count()
                .saturating_add(node_details.slot_count())
                .saturating_add(edge_details.slot_count())
                .saturating_add(endpoints.slot_count())
                .saturating_add(
                    row_groups
                        .values()
                        .map(RowMergeAccumulator::slot_count)
                        .sum::<usize>(),
                );
            self.checkpoint.evidence.peak_merge_name_slots = self
                .checkpoint
                .evidence
                .peak_merge_name_slots
                .max(retained_names as u64);
        }
        let staged_identities = unified.finish_optional::<BASE_IDENTITY_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let staged_identities =
            staged_identities.ok_or_else(|| storage("construction contains no identities"))?;
        let node_details = node_details.finish_optional::<NODE_DETAIL_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let edge_details = edge_details.finish_optional::<EDGE_DETAIL_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let endpoints = endpoints.finish_optional::<ENDPOINT_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let (base_max_node, base_max_edge) = match self.checkpoint.parent_topology_generation {
            0 => (0, 0),
            _ => (if let Some(inventory) = &self.compact_parent {
                compact_parent_surrogate_tails(&self.project_path, inventory)?
            } else {
                None
            })
            .or(crate::writer::read_surrogate_tails(
                // The retained project directory is revalidated immediately above.
                &self.project_path,
            )?)
            .ok_or_else(|| storage("nonempty parent lacks surrogate tails"))?,
        };
        if base_max_node != self.checkpoint.base_work.max_node_surrogate {
            return Err(storage("UUID snapshot and surrogate tails disagree"));
        }
        let (new_nodes, new_edges) = validate_staged_details(
            &self.root,
            &staged_identities,
            node_details.as_deref(),
            edge_details.as_deref(),
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        if let Some(base) = self.base_snapshot.as_mut() {
            reject_staged_base_conflicts(
                &self.root,
                &staged_identities,
                base,
                self.checkpoint.budgets.max_batch_rows,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
        }
        let identities = assign_surrogates(
            &self.root,
            &staged_identities,
            base_max_node,
            base_max_edge,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let edge_endpoints = resolve_endpoint_surrogates(
            &self.root,
            &identities,
            endpoints.as_deref(),
            self.base_snapshot.as_mut(),
            self.checkpoint.budgets.max_batch_rows,
            self.checkpoint.budgets.merge_fan_in,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let node_count = self
            .checkpoint
            .base_work
            .live_nodes
            .saturating_add(new_nodes);
        let edge_count = self
            .checkpoint
            .base_work
            .live_edges
            .saturating_add(new_edges);
        let max_node_surrogate = base_max_node
            .checked_add(new_nodes)
            .ok_or_else(|| storage("node surrogate overflow"))?;
        let max_edge_surrogate = base_max_edge
            .checked_add(new_edges)
            .ok_or_else(|| storage("edge surrogate overflow"))?;
        let mut node_rows = Vec::new();
        let mut edge_rows = Vec::new();
        for ((kind, schema_digest), rows) in row_groups {
            reject_cancelled(&mut cancelled)?;
            let output = format!("shaped-rows-{kind}-{schema_digest}.parquet");
            rows.finish(
                &self.root,
                &output,
                self.checkpoint.budgets.max_batch_rows,
                self.checkpoint.budgets.max_batch_bytes,
                self.checkpoint.budgets.merge_fan_in,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            if kind == 0 {
                node_rows.push(output);
            } else {
                edge_rows.push(output);
            }
        }
        let runtime_catalog = build_runtime_catalog(
            self.parent_catalog.clone(),
            &self.root,
            &node_rows,
            &edge_rows,
            self.checkpoint.session_now_micros,
            self.checkpoint.budgets,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        self.checkpoint.evidence.peak_merge_temporary_bytes = self
            .checkpoint
            .evidence
            .peak_merge_temporary_bytes
            .max(measured_shape_bytes(&self.root)?);
        let shape = ConstructionShape {
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            parent_uuid_manifest_sha256: self
                .base_snapshot
                .as_ref()
                .map(|snapshot| snapshot.manifest_sha256().to_owned()),
            identities,
            node_details,
            edge_details,
            node_rows,
            edge_rows,
            edge_endpoints,
            runtime_catalog_now_micros: self.checkpoint.session_now_micros,
            runtime_catalog_inputs_sha256: hex(&catalog_authority.finalize()),
            runtime_catalog,
            node_count,
            edge_count,
            max_node_surrogate,
            max_edge_surrogate,
        };
        let (identity_output, mut inventory_work) =
            receipt_for_existing_with_work(&self.root, &shape.identities)?;
        let mut outputs = vec![identity_output];
        for name in shape
            .node_details
            .iter()
            .chain(shape.edge_details.iter())
            .chain(shape.node_rows.iter())
            .chain(shape.edge_rows.iter())
            .chain(shape.edge_endpoints.iter())
            .chain(std::iter::once(&shape.runtime_catalog))
        {
            let (output, work) = receipt_for_existing_with_work(&self.root, name)?;
            inventory_work.bytes = inventory_work.bytes.saturating_add(work.bytes);
            inventory_work.operations = inventory_work.operations.saturating_add(work.operations);
            outputs.push(output);
        }
        self.checkpoint.evidence.shaped_output_authentication_bytes = self
            .checkpoint
            .evidence
            .shaped_output_authentication_bytes
            .saturating_add(inventory_work.bytes);
        self.checkpoint
            .evidence
            .shaped_output_authentication_operations = self
            .checkpoint
            .evidence
            .shaped_output_authentication_operations
            .saturating_add(inventory_work.operations);
        self.checkpoint.evidence.shape_application_read_bytes = self
            .checkpoint
            .evidence
            .shape_input_validation_read_bytes
            .saturating_add(self.checkpoint.evidence.merge_read_bytes)
            .saturating_add(self.checkpoint.evidence.parquet_read_bytes)
            .saturating_add(self.checkpoint.evidence.shaped_output_authentication_bytes)
            .saturating_add(self.checkpoint.evidence.parent_catalog_read_bytes)
            .saturating_add(self.checkpoint.evidence.retained_probe_read_bytes);
        let shape_authority_sha256 = shape_authority_sha256(&shape, &outputs)?;
        self.checkpoint.shape_authority_sha256 = Some(shape_authority_sha256.clone());
        replace_control(
            &self.root,
            SHAPE_INTENT,
            &ShapeIntent {
                format_version: FORMAT_VERSION,
                operation_uuid: self.checkpoint.operation_uuid,
                project_identity: self.checkpoint.project_identity.clone(),
                session_identity: self.checkpoint.session_identity.clone(),
                parent_topology_generation: self.checkpoint.parent_topology_generation,
                ontology_mode: self.checkpoint.ontology_mode,
                semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
                budgets: self.checkpoint.budgets,
                last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
                baseline_evidence: shape_intent.baseline_evidence,
                final_evidence: Some(self.checkpoint.evidence.clone()),
                complete: true,
                shape: Some(shape.clone()),
                outputs,
                shape_authority_sha256: Some(shape_authority_sha256),
            },
        )?;
        construction_failpoint("shape.after_complete_inventory");
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)?;
        construction_failpoint("shape.after_evidence_checkpoint");
        Ok(shape)
    }

    /// Abort before seal. CURRENT remains unchanged.
    pub fn abort(&mut self) -> Result<(), GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("non-staging session belongs to the publisher"));
        }
        self.checkpoint.state = GraphConstructionState::Aborted;
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
    }

    #[allow(clippy::too_many_lines)]
    fn recover_intent(&mut self) -> Result<(), GfError> {
        let mut file = match self.root.open_child_file(OsStr::new(INTENT)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(storage(error)),
        };
        let intent: ChunkIntent = decode_bounded(&mut file)?;
        validate_intent(&intent, &self.checkpoint)?;
        let receipt_name = receipt_name(intent.sequence);
        match self.root.open_child_file(OsStr::new(&receipt_name)) {
            Ok(mut receipt_file) => {
                let receipt: ConstructionChunkReceipt = decode_bounded(&mut receipt_file)?;
                validate_receipt_semantics(&receipt, intent.sequence, self.checkpoint.budgets)?;
                if receipt != receipt_from_intent(&intent)? {
                    return Err(storage("recovered receipt differs from durable intent"));
                }
                validate_receipt_artifacts(&self.root, &receipt)?;
                let body = serde_json::to_vec(&receipt).map_err(storage)?;
                if intent.sequence < self.checkpoint.next_sequence
                    && self.checkpoint.last_receipt_sha256.as_deref()
                        != Some(sha256(&body).as_str())
                {
                    return Err(storage(
                        "completed intent receipt differs from checkpoint tail",
                    ));
                }
                let expected_pointer = ReceiptPointer {
                    operation_uuid: self.checkpoint.operation_uuid,
                    project_identity: self.checkpoint.project_identity.clone(),
                    session_identity: self.checkpoint.session_identity.clone(),
                    sequence: receipt.sequence,
                    receipt_sha256: sha256(&body),
                };
                match self.root.open_child_file(OsStr::new(&intent.chunk_key)) {
                    Ok(mut pointer_file) => {
                        let pointer: ReceiptPointer = decode_bounded(&mut pointer_file)?;
                        if pointer.operation_uuid != expected_pointer.operation_uuid
                            || pointer.project_identity != expected_pointer.project_identity
                            || pointer.session_identity != expected_pointer.session_identity
                            || pointer.sequence != expected_pointer.sequence
                            || pointer.receipt_sha256 != expected_pointer.receipt_sha256
                        {
                            return Err(storage("recovered receipt pointer is inconsistent"));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        install_control(&self.root, &intent.chunk_key, &expected_pointer)?;
                    }
                    Err(error) => return Err(storage(error)),
                }
                self.advance_checkpoint(&receipt, &body)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for artifact in [
                    intent.parquet.clone(),
                    intent.identities.clone(),
                    intent.endpoints.clone(),
                    intent.details.clone(),
                ]
                .into_iter()
                .flatten()
                {
                    authenticate_artifact(&self.root, &artifact)?;
                    unlink_artifact(&self.root, &artifact)?;
                }
                let stem = artifact_stem(intent.sequence, intent.kind);
                if intent.parquet.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.parquet"),
                        intent.kind,
                        intent.rows,
                    )?;
                }
                if intent.identities.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.identities.run"),
                        intent.kind,
                        intent.rows,
                    )?;
                }
                if intent.kind == ConstructionChunkKind::Edge && intent.endpoints.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.endpoints.run"),
                        intent.kind,
                        intent.rows,
                    )?;
                }
                if intent.details.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!(
                            "{stem}.{}-details.run",
                            if intent.kind == ConstructionChunkKind::Node {
                                "node"
                            } else {
                                "edge"
                            }
                        ),
                        intent.kind,
                        intent.rows,
                    )?;
                }
            }
            Err(error) => return Err(storage(error)),
        }
        unlink_named(&self.root, INTENT)
    }

    fn read_receipt(&self, sequence: u64) -> Result<ConstructionChunkReceipt, GfError> {
        let mut file = self
            .root
            .open_child_file(OsStr::new(&receipt_name(sequence)))
            .map_err(storage)?;
        let receipt = decode_bounded(&mut file)?;
        validate_receipt_semantics(&receipt, sequence, self.checkpoint.budgets)?;
        if receipt.operation_uuid != self.checkpoint.operation_uuid
            || receipt.project_identity != self.checkpoint.project_identity
            || receipt.session_identity != self.checkpoint.session_identity
            || receipt.parent_topology_generation != self.checkpoint.parent_topology_generation
            || receipt.ontology_mode != self.checkpoint.ontology_mode
            || receipt.semantic_authority_sha256 != self.checkpoint.semantic_authority_sha256
        {
            return Err(storage("receipt authority differs from session checkpoint"));
        }
        Ok(receipt)
    }

    fn advance_checkpoint(
        &mut self,
        receipt: &ConstructionChunkReceipt,
        receipt_bytes: &[u8],
    ) -> Result<(), GfError> {
        if receipt.sequence < self.checkpoint.next_sequence {
            return Ok(());
        }
        if receipt.sequence != self.checkpoint.next_sequence {
            return Err(storage("receipt sequence is not checkpoint successor"));
        }
        let evidence = &mut self.checkpoint.evidence;
        evidence.input_rows = evidence.input_rows.saturating_add(receipt.rows);
        evidence.input_batches = evidence.input_batches.saturating_add(1);
        evidence.parquet_shards = evidence.parquet_shards.saturating_add(1);
        evidence.run_records = evidence.run_records.saturating_add(receipt.run_records);
        evidence.peak_batch_rows = evidence.peak_batch_rows.max(receipt.rows);
        evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(receipt.input_bytes);
        evidence.peak_run_records = evidence.peak_run_records.max(receipt.run_records);
        evidence.peak_accounted_live_bytes = evidence
            .peak_accounted_live_bytes
            .max(receipt.accounted_live_bytes);
        for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
            .into_iter()
            .chain(receipt.endpoints.iter())
        {
            evidence.write_bytes = evidence.write_bytes.saturating_add(artifact.bytes);
            evidence.write_operations = evidence
                .write_operations
                .saturating_add(artifact.write_operations);
            evidence.fsync_operations = evidence
                .fsync_operations
                .saturating_add(artifact.fsync_operations);
        }
        self.checkpoint.next_sequence = self.checkpoint.next_sequence.saturating_add(1);
        self.checkpoint.saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
        match receipt.kind {
            ConstructionChunkKind::Node => {
                self.checkpoint
                    .node_schema_sha256
                    .insert(receipt.schema_sha256.clone());
            }
            ConstructionChunkKind::Edge => {
                self.checkpoint
                    .edge_schema_sha256
                    .insert(receipt.schema_sha256.clone());
            }
        }
        self.checkpoint.last_receipt_sha256 = Some(sha256(receipt_bytes));
        replace_control(&self.root, CHECKPOINT, &self.checkpoint)
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReceiptPointer {
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    sequence: u64,
    receipt_sha256: String,
}

struct RunArrays {
    identities: Vec<[u8; IDENTITY_WIDTH]>,
    endpoints: Vec<[u8; ENDPOINT_WIDTH]>,
    details: DetailRuns,
}

enum DetailRuns {
    Node(Vec<[u8; NODE_DETAIL_WIDTH]>),
    Edge(Vec<[u8; EDGE_DETAIL_WIDTH]>),
}

fn extract_runs(kind: ConstructionChunkKind, batch: &RecordBatch) -> Result<RunArrays, GfError> {
    let identity = uuid_column(
        batch,
        if kind == ConstructionChunkKind::Node {
            "node_uuid"
        } else {
            "edge_uuid"
        },
    )?;
    let mut identities = (0..batch.num_rows())
        .map(|row| uuid_value(identity, row))
        .collect::<Result<Vec<_>, _>>()?;
    identities.sort_unstable();
    if identities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage("duplicate identity inside chunk"));
    }
    let mut endpoints = Vec::new();
    let details = if kind == ConstructionChunkKind::Edge {
        let edges = uuid_column(batch, "edge_uuid")?;
        let src = uuid_column(batch, "source_uuid")?;
        let dst = uuid_column(batch, "target_uuid")?;
        let routes = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
        endpoints.reserve(batch.num_rows().saturating_mul(2));
        let mut details = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let edge = uuid_value(edges, row)?;
            for (role, endpoint) in [uuid_value(src, row)?, uuid_value(dst, row)?]
                .into_iter()
                .enumerate()
            {
                let mut record = [0_u8; ENDPOINT_WIDTH];
                record[..16].copy_from_slice(&endpoint);
                record[16..32].copy_from_slice(&edge);
                record[32] = u8::try_from(role).expect("endpoint role is zero or one");
                endpoints.push(record);
            }
            let route = routes.value(row).as_bytes();
            let mut detail = [0_u8; EDGE_DETAIL_WIDTH];
            detail[..16].copy_from_slice(&edge);
            detail[16..32].copy_from_slice(&uuid_value(src, row)?);
            detail[32..48].copy_from_slice(&uuid_value(dst, row)?);
            detail[48] = u8::try_from(route.len())
                .map_err(|_| storage("canonical edge route exceeds identifier bound"))?;
            detail[49..49 + route.len()].copy_from_slice(route);
            details.push(detail);
        }
        endpoints.sort_unstable();
        details.sort_unstable();
        DetailRuns::Edge(details)
    } else {
        let nodes = uuid_column(batch, "node_uuid")?;
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| storage("canonical node label is not Utf8"))?;
        let mut details = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut detail = [0_u8; NODE_DETAIL_WIDTH];
            detail[..16].copy_from_slice(&uuid_value(nodes, row)?);
            let label = labels.value(row).as_bytes();
            detail[16] = u8::try_from(label.len())
                .map_err(|_| storage("canonical node label exceeds identifier bound"))?;
            detail[17..17 + label.len()].copy_from_slice(label);
            details.push(detail);
        }
        details.sort_unstable();
        DetailRuns::Node(details)
    };
    Ok(RunArrays {
        identities,
        endpoints,
        details,
    })
}

fn validate_schema(kind: ConstructionChunkKind, batch: &RecordBatch) -> Result<(), GfError> {
    let expected = match kind {
        ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
        ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
    };
    if batch.num_columns() < expected.fields().len()
        || batch.schema().fields()[..expected.fields().len()] != expected.fields()[..]
        || batch.schema().fields()[expected.fields().len()..]
            .iter()
            .any(|field| {
                matches!(
                    field.name().as_str(),
                    "node_uuid"
                        | "label"
                        | "edge_uuid"
                        | "rel_type"
                        | "source_uuid"
                        | "target_uuid"
                ) || !graphforge_core::identifier::is_graph_identifier(field.name())
            })
    {
        return Err(storage("construction batch schema is not canonical"));
    }
    for column in &batch.columns()[..expected.fields().len()] {
        if column.null_count() != 0 {
            return Err(storage("required construction columns are non-null"));
        }
    }
    if batch.schema().fields()[expected.fields().len()..]
        .iter()
        .any(|field| !crate::schemas::property_data_type_supported(field.data_type()))
    {
        return Err(storage("unsupported construction property type"));
    }
    let identifiers = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| storage("canonical label or relation is not Utf8"))?;
    if identifiers
        .iter()
        .flatten()
        .any(|value| !is_construction_identifier(value))
    {
        return Err(storage("invalid canonical label or relation"));
    }
    Ok(())
}

fn is_construction_identifier(value: &str) -> bool {
    if graphforge_core::identifier::is_graph_identifier(value) {
        return true;
    }
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [module, local] => {
            graphforge_core::identifier::is_graph_identifier(module)
                && graphforge_core::identifier::is_graph_identifier(local)
        }
        [module, kind, local] => {
            graphforge_core::identifier::is_graph_identifier(module)
                && matches!(*kind, "entity" | "relation")
                && graphforge_core::identifier::is_graph_identifier(local)
        }
        _ => false,
    }
}

fn logical_batch_digest(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-logical-arrow/v1\0");
    digest.update(kind.tag().as_bytes());
    digest.update((batch.num_rows() as u64).to_be_bytes());
    match kind {
        ConstructionChunkKind::Node => {
            let uuids = uuid_column(batch, "node_uuid")?;
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage("canonical node label is not Utf8"))?;
            for row in 0..batch.num_rows() {
                digest.update(uuid_value(uuids, row)?);
                let label = labels.value(row).as_bytes();
                digest.update((label.len() as u64).to_be_bytes());
                digest.update(label);
            }
        }
        ConstructionChunkKind::Edge => {
            let edge = uuid_column(batch, "edge_uuid")?;
            let src = uuid_column(batch, "source_uuid")?;
            let dst = uuid_column(batch, "target_uuid")?;
            let route = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
            for row in 0..batch.num_rows() {
                digest.update(uuid_value(edge, row)?);
                digest.update(uuid_value(src, row)?);
                digest.update(uuid_value(dst, row)?);
                let value = route.value(row).as_bytes();
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value);
            }
        }
    }
    let required = if kind == ConstructionChunkKind::Node {
        2
    } else {
        4
    };
    let schema = batch.schema();
    for (field, column) in schema.fields()[required..]
        .iter()
        .zip(expected_property_columns(kind, batch))
    {
        digest.update((field.name().len() as u64).to_be_bytes());
        digest.update(field.name().as_bytes());
        digest.update(column.data_type().to_string().as_bytes());
        for row in 0..column.len() {
            if column.is_null(row) {
                digest.update([0]);
            } else {
                digest.update([1]);
                let value = arrow::util::display::array_value_to_string(column.as_ref(), row)
                    .map_err(storage)?;
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value.as_bytes());
            }
        }
    }
    Ok(hex(&digest.finalize()))
}

fn normalized_schema_digest(schema: &Schema) -> String {
    let mut digest = Sha256::new();
    for field in schema.fields() {
        digest.update((field.name().len() as u64).to_be_bytes());
        digest.update(field.name().as_bytes());
        let data_type = format!("{:?}", field.data_type());
        digest.update((data_type.len() as u64).to_be_bytes());
        digest.update(data_type.as_bytes());
        digest.update([u8::from(field.is_nullable())]);
        let mut metadata = field.metadata().iter().collect::<Vec<_>>();
        metadata.sort_unstable();
        for (key, value) in metadata {
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key.as_bytes());
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    let mut metadata = schema.metadata().iter().collect::<Vec<_>>();
    metadata.sort_unstable();
    for (key, value) in metadata {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key.as_bytes());
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    hex(&digest.finalize())
}

fn uuid_sorted_batch(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<RecordBatch, GfError> {
    let identity = uuid_column(
        batch,
        if kind == ConstructionChunkKind::Node {
            "node_uuid"
        } else {
            "edge_uuid"
        },
    )?;
    let mut order = (0..batch.num_rows()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|&row| identity.value(row));
    let indices = UInt32Array::from(
        order
            .into_iter()
            .map(|row| u32::try_from(row).map_err(|_| storage("row index exceeds UInt32")))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(storage)
}

fn expected_property_columns(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> impl Iterator<Item = &arrow::array::ArrayRef> {
    let required = if kind == ConstructionChunkKind::Node {
        2
    } else {
        4
    };
    batch.columns()[required..].iter()
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| storage(format!("missing canonical column {name}")))?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| storage(format!("{name} is not FixedSizeBinary(16)")))?;
    if array.value_length() != 16 || array.null_count() != 0 {
        return Err(storage(format!("{name} is not non-null UUID data")));
    }
    Ok(array)
}

fn uuid_value(array: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 16], GfError> {
    array
        .value(row)
        .try_into()
        .map_err(|_| storage("UUID width changed"))
}

struct HashingWriter<W> {
    inner: W,
    digest: Sha256,
    bytes: u64,
    operations: u64,
}

#[derive(Clone, Default)]
pub(crate) struct IoCounter {
    bytes: std::sync::Arc<AtomicU64>,
    operations: std::sync::Arc<AtomicU64>,
}

impl IoCounter {
    pub(crate) fn account(&self, bytes: usize) {
        if bytes != 0 {
            self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            self.operations.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn add_to(&self, evidence: &mut GraphConstructionEvidence) {
        evidence.parquet_read_bytes = evidence
            .parquet_read_bytes
            .saturating_add(self.bytes.load(Ordering::Relaxed));
        evidence.parquet_read_operations = evidence
            .parquet_read_operations
            .saturating_add(self.operations.load(Ordering::Relaxed));
    }

    pub(crate) fn values(&self) -> (u64, u64) {
        (
            self.bytes.load(Ordering::Relaxed),
            self.operations.load(Ordering::Relaxed),
        )
    }
}

pub(crate) struct CountingRead<R> {
    inner: R,
    counter: IoCounter,
}

impl<R: Read> Read for CountingRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.counter.account(read);
        Ok(read)
    }
}

pub(crate) struct CountingChunkReader<R = File> {
    pub(crate) file: R,
    pub(crate) counter: IoCounter,
}

pub(crate) trait ConstructionFileHandle: Send + Sync {
    fn descriptor(&self) -> &File;
    fn length(&self) -> u64;
}

impl ConstructionFileHandle for File {
    fn descriptor(&self) -> &File {
        self
    }

    fn length(&self) -> u64 {
        self.metadata().map_or(0, |metadata| metadata.len())
    }
}

impl ConstructionFileHandle for crate::graph_object_store::AuthenticatedGraphObject {
    fn descriptor(&self) -> &File {
        self.as_ref()
    }

    fn length(&self) -> u64 {
        self.authenticated_length()
    }
}

impl<R: ConstructionFileHandle> Length for CountingChunkReader<R> {
    fn len(&self) -> u64 {
        self.file.length()
    }
}

impl<R: ConstructionFileHandle> ChunkReader for CountingChunkReader<R> {
    type T = CountingRead<BufReader<File>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        use std::io::{Seek, SeekFrom};

        let mut file = self.file.descriptor().try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(CountingRead {
            inner: BufReader::with_capacity(BLOCK_BYTES, file),
            counter: self.counter.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        use std::io::{Seek, SeekFrom};

        let mut file = self.file.descriptor().try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        let mut value = vec![0_u8; length];
        file.read_exact(&mut value)?;
        self.counter.account(length);
        Ok(bytes::Bytes::from(value))
    }
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
            operations: 0,
        }
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.bytes = self.bytes.saturating_add(written as u64);
        self.operations = self.operations.saturating_add(1);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn write_parquet(
    root: &StableDirectory,
    name: &str,
    batch: &RecordBatch,
) -> Result<ArtifactReceipt, GfError> {
    let temporary = artifact_temp(name);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::new(file);
    let buffered = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut parquet = ArrowWriter::try_new(buffered, batch.schema(), None).map_err(storage)?;
    parquet.write(batch).map_err(storage)?;
    parquet.finish().map_err(storage)?;
    parquet.sync().map_err(storage)?;
    let hashing = parquet.inner().get_ref();
    hashing.inner.sync_all().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_temp_fsync.{name}"));
    let receipt = ArtifactReceipt {
        name: name.to_owned(),
        bytes: hashing.bytes,
        sha256: hex(&hashing.digest.clone().finalize()),
        identity: identity.into(),
        write_operations: hashing.operations,
        fsync_operations: 4,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_install.{name}"));
    persist_shape_receipt(root, &receipt)?;
    Ok(receipt)
}

fn write_fixed_run<const N: usize>(
    root: &StableDirectory,
    name: &str,
    records: &[[u8; N]],
) -> Result<ArtifactReceipt, GfError> {
    let temporary = artifact_temp(name);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = HashingWriter::new(file);
    let records_per_block = (BLOCK_BYTES / N).max(1);
    let mut block = Vec::with_capacity(records_per_block * N);
    for group in records.chunks(records_per_block) {
        block.clear();
        for record in group {
            block.extend_from_slice(record);
        }
        writer.write_all(&block).map_err(storage)?;
    }
    writer.flush().map_err(storage)?;
    writer.inner.sync_all().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_temp_fsync.{name}"));
    let receipt = ArtifactReceipt {
        name: name.to_owned(),
        bytes: writer.bytes,
        sha256: hex(&writer.digest.finalize()),
        identity: identity.into(),
        write_operations: writer.operations,
        fsync_operations: 3,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_install.{name}"));
    persist_shape_receipt(root, &receipt)?;
    Ok(receipt)
}

fn reject_existing_merge_artifacts(root: &StableDirectory) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if name.starts_with("merge-") || name.starts_with("shaped-") {
            return Err(storage("unowned construction shaping artifact exists"));
        }
    }
    Ok(())
}

fn recover_shape_intent(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
) -> Result<(), GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let intent: ShapeIntent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&intent, checkpoint)?;
    if intent.complete {
        let shape = intent
            .shape
            .as_ref()
            .ok_or_else(|| storage("complete shape manifest lacks output"))?;
        if shape.ontology_mode != checkpoint.ontology_mode
            || shape.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
            || shape.runtime_catalog_now_micros != checkpoint.session_now_micros
            || !is_canonical_sha256(&shape.runtime_catalog_inputs_sha256)
            || std::iter::once(&shape.identities)
                .chain(shape.node_details.iter())
                .chain(shape.edge_details.iter())
                .chain(shape.node_rows.iter())
                .chain(shape.edge_rows.iter())
                .chain(shape.edge_endpoints.iter())
                .chain(std::iter::once(&shape.runtime_catalog))
                .any(|name| !intent.outputs.iter().any(|item| item.name == *name))
        {
            return Err(storage("complete shape manifest inventory is incomplete"));
        }
        for output in &intent.outputs {
            authenticate_shaped_output(root, output)?;
        }
        let expected_shape_authority = shape_authority_sha256(shape, &intent.outputs)?;
        if intent.shape_authority_sha256.as_deref() != Some(&expected_shape_authority) {
            return Err(storage(
                "complete shape authority digest differs from inventory",
            ));
        }
        let final_evidence = intent
            .final_evidence
            .as_ref()
            .ok_or_else(|| storage("complete shape manifest lacks final evidence"))?;
        if checkpoint.evidence == intent.baseline_evidence
            && checkpoint.shape_authority_sha256.is_none()
        {
            checkpoint.evidence = final_evidence.clone();
            checkpoint.shape_authority_sha256 = Some(expected_shape_authority);
            replace_control(root, CHECKPOINT, checkpoint)?;
        } else {
            let mut shape_owned_evidence = checkpoint.evidence.clone();
            shape_owned_evidence.encode_application_read_bytes =
                final_evidence.encode_application_read_bytes;
            shape_owned_evidence.publication_application_read_bytes =
                final_evidence.publication_application_read_bytes;
            shape_owned_evidence.cas_application_read_bytes =
                final_evidence.cas_application_read_bytes;
            shape_owned_evidence.hydration_application_read_bytes =
                final_evidence.hydration_application_read_bytes;
            shape_owned_evidence.canonical_output_bytes = final_evidence.canonical_output_bytes;
            shape_owned_evidence.staged_and_retained_disk_bytes =
                final_evidence.staged_and_retained_disk_bytes;
            if shape_owned_evidence == *final_evidence
                && checkpoint.shape_authority_sha256.as_deref() == Some(&expected_shape_authority)
            {
                return Ok(());
            }
            return Err(storage("shape evidence authority differs from inventory"));
        }
        return Ok(());
    }
    if intent.shape.is_some() || !intent.outputs.is_empty() {
        return Err(storage("incomplete shape intent claims completed output"));
    }
    if intent.final_evidence.is_some() || checkpoint.evidence != intent.baseline_evidence {
        return Err(storage("incomplete shape changed committed evidence"));
    }
    cleanup_incomplete_shape_capabilities(root)?;
    for child in root.child_names().map_err(storage)? {
        let Some(name) = child.to_str() else { continue };
        if !name.starts_with("merge-") && !name.starts_with("shaped-") {
            continue;
        }
        match root.open_child_file(OsStr::new(name)) {
            Ok(file) => {
                if file_link_count(&file).map_err(storage)? != 1 {
                    return Err(storage("construction shape artifact has extra links"));
                }
                drop(file);
                unlink_named(root, name)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage(error)),
        }
    }
    unlink_named(root, SHAPE_INTENT)
}

fn cleanup_incomplete_shape_capabilities(root: &StableDirectory) -> Result<(), GfError> {
    for child in root.child_names().map_err(storage)? {
        let Some(name) = child.to_str() else { continue };
        if !name.starts_with("shape-receipt-") {
            continue;
        }
        let mut file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("shaped writer capability has extra links"));
        }
        let identity = file_identity(&file).map_err(storage)?;
        let receipt: ArtifactReceipt = decode_bounded(&mut file)?;
        if shape_receipt_name(&receipt.name) != name {
            return Err(storage("shaped writer capability name changed"));
        }
        if !is_shape_artifact_name(&receipt.name) {
            continue;
        }
        if !is_canonical_sha256(&receipt.sha256) {
            return Err(storage("shaped writer capability digest changed"));
        }
        match root.open_child_file(OsStr::new(&receipt.name)) {
            Ok(artifact) => {
                drop(artifact);
                authenticate_shaped_output(root, &receipt)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage(error)),
        }
        drop(file);
        root.unlink_child_if_identity(OsStr::new(name), identity)
            .map_err(storage)?;
        root.sync().map_err(storage)?;
    }
    Ok(())
}

fn validate_shape_binding(intent: &ShapeIntent, checkpoint: &Checkpoint) -> Result<(), GfError> {
    if intent.format_version != FORMAT_VERSION
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || intent.parent_topology_generation != checkpoint.parent_topology_generation
        || intent.ontology_mode != checkpoint.ontology_mode
        || intent.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
        || intent.budgets != checkpoint.budgets
        || intent.last_receipt_sha256 != checkpoint.last_receipt_sha256
    {
        return Err(storage("construction shape manifest authority changed"));
    }
    Ok(())
}

fn control_sha256(value: &impl Serialize) -> Result<String, GfError> {
    Ok(sha256(&serde_json::to_vec(value).map_err(storage)?))
}

fn validate_sha256(value: &str, label: &str) -> Result<(), GfError> {
    if !is_canonical_sha256(value) {
        return Err(storage(format!("{label} digest is invalid")));
    }
    Ok(())
}

fn is_canonical_sha256(value: &str) -> bool {
    is_canonical_lower_hex(value, 64)
}

fn is_canonical_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_publication_intent(
    intent: &ConstructionPublicationIntent,
    checkpoint: &Checkpoint,
) -> Result<(), GfError> {
    validate_sha256(
        &intent.parent_generation_manifest_sha256,
        "parent generation manifest",
    )?;
    validate_sha256(&intent.shape_authority_sha256, "shape authority")?;
    validate_sha256(&intent.encoding_inventory_sha256, "encoding inventory")?;
    if intent.format_version != FORMAT_VERSION
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || intent.parent_generation_uuid != checkpoint.parent_generation_uuid
        || intent.parent_generation_manifest_sha256 != checkpoint.parent_generation_manifest_sha256
        || intent.shape_authority_sha256
            != checkpoint
                .shape_authority_sha256
                .clone()
                .unwrap_or_default()
        || intent.encoding_inventory_sha256
            != checkpoint
                .encoding_inventory_sha256
                .clone()
                .unwrap_or_default()
        || intent.target_generation_uuid.is_nil()
        || intent.transaction_uuid.is_nil()
    {
        return Err(storage("publication intent authority changed"));
    }
    Ok(())
}

fn read_publication_intent(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<ConstructionPublicationIntent, GfError> {
    let mut file = root
        .open_child_file(OsStr::new(PUBLICATION_INTENT))
        .map_err(storage)?;
    let intent = decode_bounded(&mut file)?;
    validate_publication_intent(&intent, checkpoint)?;
    Ok(intent)
}

fn read_publication_receipt(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<ConstructionPublicationReceipt, GfError> {
    let intent = read_publication_intent(root, checkpoint)?;
    let mut file = root
        .open_child_file(OsStr::new(PUBLICATION_RECEIPT))
        .map_err(storage)?;
    let receipt: ConstructionPublicationReceipt = decode_bounded(&mut file)?;
    validate_sha256(
        &receipt.target_generation_manifest_sha256,
        "target generation manifest",
    )?;
    if receipt.intent_sha256 != control_sha256(&intent)?
        || receipt.operation_uuid != checkpoint.operation_uuid
        || receipt.project_identity != checkpoint.project_identity
        || receipt.session_identity != checkpoint.session_identity
        || receipt.transaction_uuid != intent.transaction_uuid
        || receipt.target_generation_uuid != intent.target_generation_uuid
    {
        return Err(storage("publication receipt differs from durable intent"));
    }
    Ok(receipt)
}

fn authenticate_published_target(
    project_dir: &Path,
    checkpoint: &Checkpoint,
    receipt: &ConstructionPublicationReceipt,
) -> Result<(), GfError> {
    let target = crate::resolve_generation_by_uuid(project_dir, receipt.target_generation_uuid)
        .map_err(|error| storage(format!("published target cannot be authenticated: {error}")))?;
    let actual_manifest = hex(&target.manifest_sha256());
    if actual_manifest != receipt.target_generation_manifest_sha256 {
        return Err(storage(
            "published target manifest differs from durable receipt",
        ));
    }
    if target.parent_generation_uuid() != Some(checkpoint.parent_generation_uuid) {
        return Err(storage(
            "published target is not a child of the pinned parent generation",
        ));
    }
    let journal = crate::published_project_transaction(project_dir, receipt.transaction_uuid)
        .map_err(|error| storage(format!("project publication journal is invalid: {error}")))?
        .ok_or_else(|| storage("project publication journal is not published"))?;
    if journal.transaction_uuid != receipt.transaction_uuid
        || journal.generation_uuid != receipt.target_generation_uuid
        || journal.generation_manifest_sha256 != target.manifest_sha256()
    {
        return Err(storage(
            "project publication journal differs from construction publication authority",
        ));
    }
    Ok(())
}

fn recover_publication(
    project_dir: &Path,
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
) -> Result<(), GfError> {
    let intent = match root.open_child_file(OsStr::new(PUBLICATION_INTENT)) {
        Ok(mut file) => {
            let intent: ConstructionPublicationIntent = decode_bounded(&mut file)?;
            validate_publication_intent(&intent, checkpoint)?;
            Some(intent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(storage(error)),
    };
    if intent.is_some()
        && checkpoint.publication_state == Some(ConstructionPublicationState::Sealed)
    {
        checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
        replace_control(root, CHECKPOINT, checkpoint)?;
    }
    let receipt_exists = match root.open_child_file(OsStr::new(PUBLICATION_RECEIPT)) {
        Ok(file) => {
            drop(file);
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(storage(error)),
    };
    if receipt_exists {
        let receipt = read_publication_receipt(root, checkpoint)?;
        authenticate_published_target(project_dir, checkpoint, &receipt)?;
        if checkpoint.publication_state == Some(ConstructionPublicationState::Publishing) {
            checkpoint.publication_state = Some(ConstructionPublicationState::Published);
            replace_control(root, CHECKPOINT, checkpoint)?;
        }
    }
    match checkpoint.publication_state {
        Some(ConstructionPublicationState::Publishing) if intent.is_none() => {
            Err(storage("publishing checkpoint lacks durable intent"))
        }
        Some(ConstructionPublicationState::Published) if !receipt_exists => {
            Err(storage("published checkpoint lacks durable receipt"))
        }
        None | Some(ConstructionPublicationState::Sealed) if intent.is_some() || receipt_exists => {
            Err(storage(
                "publication metadata is inconsistent with session state",
            ))
        }
        _ => Ok(()),
    }
}

fn read_completed_shape(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
    authenticate_outputs: bool,
) -> Result<Option<ConstructionShape>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    let manifest: ShapeIntent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&manifest, checkpoint)?;
    if !manifest.complete {
        return Err(storage("incomplete construction shape was not recovered"));
    }
    if authenticate_outputs {
        for output in &manifest.outputs {
            authenticate_shaped_output(root, output)?;
        }
    }
    let shape = manifest
        .shape
        .ok_or_else(|| storage("complete shape manifest lacks output"))?;
    let authority = shape_authority_sha256(&shape, &manifest.outputs)?;
    if manifest.shape_authority_sha256.as_deref() != Some(&authority)
        || checkpoint.shape_authority_sha256.as_deref() != Some(&authority)
    {
        return Err(storage("completed shape authority digest changed"));
    }
    Ok(Some(shape))
}

fn read_completed_shape_outputs(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<Vec<ArtifactReceipt>, GfError> {
    let mut file = root
        .open_child_file(OsStr::new(SHAPE_INTENT))
        .map_err(storage)?;
    let manifest: ShapeIntent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&manifest, checkpoint)?;
    if !manifest.complete || manifest.shape.is_none() {
        return Err(storage("complete construction shape inventory is absent"));
    }
    Ok(manifest.outputs)
}

pub(crate) fn shaped_output_sha256<'a>(
    outputs: &'a [ArtifactReceipt],
    name: &str,
) -> Result<&'a str, GfError> {
    outputs
        .iter()
        .find(|output| output.name == name)
        .map(|output| output.sha256.as_str())
        .ok_or_else(|| storage("shaped output receipt is absent"))
}

pub(crate) struct AuthenticatedShapeSource {
    pub(crate) file: File,
    pub(crate) identity: FileIdentity,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

pub(crate) fn open_authenticated_shape_source(
    root: &StableDirectory,
    outputs: &[ArtifactReceipt],
    name: &str,
) -> Result<AuthenticatedShapeSource, GfError> {
    let expected = outputs
        .iter()
        .find(|output| output.name == name)
        .ok_or_else(|| storage("shaped output receipt is absent"))?;
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("shaped output has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    if !expected.identity.matches(identity) {
        return Err(storage("shaped output identity changed before consumption"));
    }
    Ok(AuthenticatedShapeSource {
        file,
        identity,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
    })
}

fn receipt_for_existing(root: &StableDirectory, name: &str) -> Result<ArtifactReceipt, GfError> {
    receipt_for_existing_with_work(root, name).map(|(receipt, _)| receipt)
}

fn receipt_for_existing_with_work(
    root: &StableDirectory,
    name: &str,
) -> Result<(ArtifactReceipt, ReadWork), GfError> {
    let capability_name = shape_receipt_name(name);
    if let Ok(mut capability_file) = root.open_child_file(OsStr::new(&capability_name)) {
        let control_bytes = capability_file.metadata().map_err(storage)?.len();
        let receipt: ArtifactReceipt = decode_bounded(&mut capability_file)?;
        if receipt.name != name {
            return Err(storage("shaped writer capability names another artifact"));
        }
        let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1
            || !receipt
                .identity
                .matches(file_identity(&file).map_err(storage)?)
            || file.metadata().map_err(storage)?.len() != receipt.bytes
        {
            return Err(storage("shaped writer capability identity changed"));
        }
        return Ok((
            receipt,
            ReadWork {
                bytes: control_bytes,
                operations: 1,
            },
        ));
    }
    let mut file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("shaped output has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut operations = 0_u64;
    let mut block = vec![0_u8; BLOCK_BYTES];
    loop {
        let count = file.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
        bytes = bytes.saturating_add(count as u64);
        operations = operations.saturating_add(1);
    }
    Ok((
        ArtifactReceipt {
            name: name.to_owned(),
            bytes,
            sha256: hex(&digest.finalize()),
            identity: identity.into(),
            write_operations: 0,
            fsync_operations: 0,
        },
        ReadWork { bytes, operations },
    ))
}

fn shape_receipt_name(name: &str) -> String {
    format!("shape-receipt-{}.json", &sha256(name.as_bytes())[..32])
}

fn persist_shape_receipt(root: &StableDirectory, receipt: &ArtifactReceipt) -> Result<(), GfError> {
    if is_shape_artifact_name(&receipt.name) || canonical_artifact_target(&receipt.name) {
        let capability_name = shape_receipt_name(&receipt.name);
        match root.open_child_file(OsStr::new(&capability_name)) {
            Ok(mut file) => {
                if file_link_count(&file).map_err(storage)? != 1 {
                    return Err(storage("shaped writer capability has extra links"));
                }
                let existing: ArtifactReceipt = decode_bounded(&mut file)?;
                if existing != *receipt {
                    return Err(storage("shaped writer capability changed"));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                install_control(root, &capability_name, receipt)?;
            }
            Err(error) => return Err(storage(error)),
        }
    }
    Ok(())
}

fn unlink_writer_capability(
    root: &StableDirectory,
    name: &str,
    expected: Option<&ArtifactReceipt>,
) -> Result<(), GfError> {
    let capability_name = shape_receipt_name(name);
    let mut file = match root.open_child_file(OsStr::new(&capability_name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("writer capability has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let receipt: ArtifactReceipt = decode_bounded(&mut file)?;
    if receipt.name != name
        || shape_receipt_name(&receipt.name) != capability_name
        || expected.is_some_and(|expected| expected != &receipt)
    {
        return Err(storage("writer capability differs from artifact authority"));
    }
    authenticate_shaped_output(root, &receipt)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(&capability_name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn authenticate_shaped_output(
    root: &StableDirectory,
    expected: &ArtifactReceipt,
) -> Result<(), GfError> {
    if !is_shape_artifact_name(&expected.name) && !canonical_artifact_target(&expected.name) {
        return Err(storage("shape manifest output name is not canonical"));
    }
    let actual = receipt_for_existing(root, &expected.name)?;
    if actual.bytes != expected.bytes
        || actual.sha256 != expected.sha256
        || actual.identity != expected.identity
    {
        return Err(storage("shape manifest output authentication changed"));
    }
    Ok(())
}

fn is_shape_artifact_name(name: &str) -> bool {
    if name == "shaped-identities.run" || name == "shaped-runtime-catalog.parquet" {
        return true;
    }
    if let Some(body) = name
        .strip_prefix("shaped-rows-")
        .and_then(|body| body.strip_suffix(".parquet"))
    {
        return body.len() == 66
            && matches!(body.as_bytes().first(), Some(b'0' | b'1'))
            && body.as_bytes().get(1) == Some(&b'-')
            && is_canonical_sha256(&body[2..]);
    }
    if let Some(body) = name
        .strip_prefix("merge-rows-")
        .and_then(|body| body.strip_suffix(".parquet"))
    {
        let mut parts = body.split("-l");
        let namespace = parts.next().unwrap_or_default();
        let level_group = parts.next().unwrap_or_default();
        return parts.next().is_none()
            && namespace.len() == 16
            && is_canonical_lower_hex(namespace, 16)
            && level_group.split_once("-g").is_some_and(|(level, group)| {
                level.len() == 3
                    && level.bytes().all(|byte| byte.is_ascii_digit())
                    && group.len() == 20
                    && group.bytes().all(|byte| byte.is_ascii_digit())
            });
    }
    if let Some(sequence) = name
        .strip_prefix("merge-unified-")
        .and_then(|body| body.strip_suffix(".run"))
    {
        return sequence.len() == 20 && sequence.bytes().all(|byte| byte.is_ascii_digit());
    }
    for prefix in [
        "merge-node-source-",
        "merge-edge-source-",
        "merge-endpoint-source-",
        "merge-resolved-source-",
    ] {
        if let Some(sequence) = name
            .strip_prefix(prefix)
            .and_then(|body| body.strip_suffix(".run"))
        {
            return sequence.len() == 20 && sequence.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    [
        "merge-identities-l",
        "merge-node-details-l",
        "merge-edge-details-l",
        "merge-endpoints-l",
        "merge-resolved-l",
    ]
    .iter()
    .any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|tail| {
            tail.len() == 17
                && tail.get(3..5) == Some("-g")
                && tail.get(13..) == Some(".run")
                && tail[..3].bytes().all(|byte| byte.is_ascii_digit())
                && tail[5..13].bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

fn measured_shape_bytes(root: &StableDirectory) -> Result<u64, GfError> {
    let mut bytes = 0_u64;
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if is_shape_artifact_name(name) {
            bytes = bytes.saturating_add(
                root.open_child_file(OsStr::new(name))
                    .map_err(storage)?
                    .metadata()
                    .map_err(storage)?
                    .len(),
            );
        }
    }
    Ok(bytes)
}

fn account_merge_read<const N: usize>(evidence: &mut GraphConstructionEvidence) {
    evidence.merge_read_records = evidence.merge_read_records.saturating_add(1);
    evidence.merge_read_bytes = evidence.merge_read_bytes.saturating_add(N as u64);
}

fn account_merge_write<const N: usize>(evidence: &mut GraphConstructionEvidence) {
    evidence.merge_written_records = evidence.merge_written_records.saturating_add(1);
    evidence.merge_written_bytes = evidence.merge_written_bytes.saturating_add(N as u64);
}

fn account_sequential_read(bytes: u64, evidence: &mut GraphConstructionEvidence) {
    if bytes != 0 {
        evidence.merge_read_blocks = evidence
            .merge_read_blocks
            .saturating_add(bytes.div_ceil(BLOCK_BYTES as u64));
    }
}

fn account_sequential_write(bytes: u64, evidence: &mut GraphConstructionEvidence) {
    if bytes != 0 {
        evidence.merge_write_blocks = evidence
            .merge_write_blocks
            .saturating_add(bytes.div_ceil(BLOCK_BYTES as u64));
    }
}

fn convert_identity_run(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
    output: &str,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&receipt.identities.name))
        .map_err(storage)?;
    account_sequential_read(receipt.identities.bytes, evidence);
    if !receipt
        .identities
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("identity source authority changed before merge"));
    }
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, input);
    let hashing = HashingWriter::new(file);
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    while let Some(uuid) = read_fixed::<IDENTITY_WIDTH>(&mut reader)? {
        digest.update(uuid);
        bytes = bytes.saturating_add(IDENTITY_WIDTH as u64);
        let mut record = [0_u8; BASE_IDENTITY_WIDTH];
        record[..16].copy_from_slice(&uuid);
        record[16] = u8::from(receipt.kind == ConstructionChunkKind::Edge);
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<IDENTITY_WIDTH>(evidence);
        account_merge_write::<BASE_IDENTITY_WIDTH>(evidence);
    }
    if bytes != receipt.identities.bytes || hex(&digest.finalize()) != receipt.identities.sha256 {
        return Err(storage("identity source content changed before merge"));
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().inner.sync_all().map_err(storage)?;
    account_sequential_write(bytes.saturating_mul(2), evidence);
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint("shape.fixed.after_install");
    evidence.merge_fsync_operations = evidence.merge_fsync_operations.saturating_add(2);
    Ok(())
}

fn copy_authenticated_run<const N: usize>(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    output: &str,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    account_sequential_read(receipt.bytes, evidence);
    if !receipt
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("construction merge source authority changed"));
    }
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, input);
    let hashing = HashingWriter::new(file);
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    while let Some(record) = read_fixed::<N>(&mut reader)? {
        digest.update(record);
        bytes = bytes.saturating_add(N as u64);
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<N>(evidence);
        account_merge_write::<N>(evidence);
    }
    if bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
        return Err(storage("construction merge source content changed"));
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().inner.sync_all().map_err(storage)?;
    account_sequential_write(bytes, evidence);
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    let output_receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes,
        sha256: receipt.sha256.clone(),
        identity: identity.into(),
        write_operations: bytes.div_ceil(BLOCK_BYTES as u64),
        fsync_operations: 2,
    };
    persist_shape_receipt(root, &output_receipt)?;
    evidence.merge_fsync_operations = evidence.merge_fsync_operations.saturating_add(2);
    Ok(())
}

/// Online external-merge scheduler.  It retains at most `fan_in - 1` names per
/// logarithmic level rather than one name per accepted chunk.
struct FixedMergeAccumulator {
    prefix: &'static str,
    fan_in: usize,
    reject_duplicates: bool,
    levels: Vec<Vec<String>>,
    groups: Vec<usize>,
    inputs: u64,
}

#[cfg(test)]
fn online_merge_name_slot_bound(mut inputs: u64, fan_in: usize) -> u64 {
    let radix = fan_in as u64;
    let mut slots = 0_u64;
    while inputs != 0 {
        slots = slots.saturating_add(inputs % radix);
        inputs /= radix;
    }
    slots.max(1)
}

impl FixedMergeAccumulator {
    fn new(prefix: &'static str, fan_in: usize, reject_duplicates: bool) -> Self {
        Self {
            prefix,
            fan_in,
            reject_duplicates,
            levels: Vec::new(),
            groups: Vec::new(),
            inputs: 0,
        }
    }

    fn slot_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    fn push<const N: usize>(
        &mut self,
        root: &StableDirectory,
        mut name: String,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        self.inputs = self.inputs.saturating_add(1);
        let mut level = 0;
        loop {
            if self.levels.len() <= level {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level].push(name);
            if self.levels[level].len() < self.fan_in {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            let group = self.groups[level];
            self.groups[level] = group.saturating_add(1);
            evidence.merge_passes = evidence.merge_passes.max(level as u64 + 1);
            name = format!("{}-l{level:03}-g{group:08}.run", self.prefix);
            merge_fixed_group::<N>(
                root,
                &inputs,
                &name,
                self.reject_duplicates,
                cancelled,
                evidence,
            )?;
            for input in inputs {
                if input.starts_with("merge-") {
                    unlink_named(root, &input)?;
                }
            }
            level += 1;
        }
    }

    fn finish_optional<const N: usize>(
        self,
        root: &StableDirectory,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<Option<String>, GfError> {
        if self.inputs == 0 {
            return Ok(None);
        }
        self.finish::<N>(root, cancelled, evidence).map(Some)
    }

    fn finish<const N: usize>(
        mut self,
        root: &StableDirectory,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<String, GfError> {
        if self.inputs == 0 {
            return Err(storage("external merge has no input"));
        }
        let mut level = 0;
        loop {
            if level >= self.levels.len() {
                return Err(storage("external merge scheduler lost its root"));
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            if inputs.is_empty() {
                level += 1;
                continue;
            }
            let higher_empty = self.levels[level + 1..].iter().all(Vec::is_empty);
            if inputs.len() == 1 && higher_empty {
                return Ok(inputs.into_iter().next().expect("one merge root"));
            }
            let name = if inputs.len() == 1 {
                inputs[0].clone()
            } else {
                let group = self.groups[level];
                self.groups[level] = group.saturating_add(1);
                evidence.merge_passes = evidence.merge_passes.max(level as u64 + 1);
                let output = format!("{}-l{level:03}-g{group:08}.run", self.prefix);
                merge_fixed_group::<N>(
                    root,
                    &inputs,
                    &output,
                    self.reject_duplicates,
                    cancelled,
                    evidence,
                )?;
                for input in inputs {
                    if input.starts_with("merge-") {
                        unlink_named(root, &input)?;
                    }
                }
                output
            };
            if self.levels.len() <= level + 1 {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level + 1].push(name);
            level += 1;
        }
    }
}

fn merge_fixed_group<const N: usize>(
    root: &StableDirectory,
    inputs: &[String],
    output: &str,
    reject_duplicates: bool,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    let mut readers = inputs
        .iter()
        .map(|name| {
            root.open_child_file(OsStr::new(name))
                .map(|file| {
                    if let Ok(metadata) = file.metadata() {
                        account_sequential_read(metadata.len(), evidence);
                    }
                    BufReader::with_capacity(BLOCK_BYTES, file)
                })
                .map_err(storage)
        })
        .collect::<Result<Vec<_>, _>>()?;
    evidence.peak_merge_inputs = evidence.peak_merge_inputs.max(readers.len() as u64);
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::new(file);
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_fixed::<N>(reader)? {
            heap.push(Reverse((record, index)));
            account_merge_read::<N>(evidence);
        }
    }
    let mut previous = None;
    while let Some(Reverse((record, index))) = heap.pop() {
        if reject_duplicates
            && previous
                .as_ref()
                .is_some_and(|prior: &[u8; N]| prior[..16] == record[..16])
        {
            return Err(storage("duplicate identity across construction runs"));
        }
        writer.write_all(&record).map_err(storage)?;
        account_merge_write::<N>(evidence);
        previous = Some(record);
        if evidence.merge_written_records.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
        if let Some(next) = read_fixed::<N>(&mut readers[index])? {
            heap.push(Reverse((next, index)));
            account_merge_read::<N>(evidence);
        }
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().inner.sync_all().map_err(storage)?;
    account_sequential_write(writer.get_ref().bytes, evidence);
    let receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes: writer.get_ref().bytes,
        sha256: hex(&writer.get_ref().digest.clone().finalize()),
        identity: identity.into(),
        write_operations: writer.get_ref().operations,
        fsync_operations: 2,
    };
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint("shape.fixed_merge.after_install");
    persist_shape_receipt(root, &receipt)?;
    evidence.merge_fsync_operations = evidence.merge_fsync_operations.saturating_add(2);
    evidence.merge_groups = evidence.merge_groups.saturating_add(1);
    evidence.peak_merge_temporary_bytes = evidence
        .peak_merge_temporary_bytes
        .max(measured_shape_bytes(root)?);
    Ok(receipt)
}

struct RowCursor {
    reader: Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>,
    batch: Option<RecordBatch>,
    row: usize,
}

impl RowCursor {
    fn advance(&mut self) -> Result<Option<[u8; 16]>, GfError> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                return uuid_value(
                    uuid_column(batch, batch.schema().field(0).name())?,
                    self.row,
                )
                .map(Some);
            }
            self.batch = self.reader.next().transpose().map_err(storage)?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

fn materialize_selected_rows(
    schema: SchemaRef,
    selected: &[(RecordBatch, usize)],
) -> Result<RecordBatch, GfError> {
    let columns = (0..schema.fields().len())
        .map(|column| {
            let data = selected
                .iter()
                .map(|(batch, _)| batch.column(column).to_data())
                .collect::<Vec<_>>();
            let refs = data.iter().collect::<Vec<_>>();
            let mut mutable = MutableArrayData::new(refs, false, selected.len());
            for (source, (_, row)) in selected.iter().enumerate() {
                mutable.extend(source, *row, row.saturating_add(1));
            }
            Ok(make_array(mutable.freeze()))
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    RecordBatch::try_new(schema, columns).map_err(storage)
}

/// Merge one exact-schema set of UUID-sorted normalized Parquet row artifacts.
/// Memory is bounded by one decoder window per input plus one caller-sized
/// output window; no property values are projected away.
#[allow(clippy::too_many_lines)]
fn merge_row_group(
    root: &StableDirectory,
    inputs: &[String],
    output: &str,
    output_rows: usize,
    output_bytes: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    if inputs.is_empty() || output_rows == 0 || output_bytes == 0 {
        return Err(storage("invalid row merge group"));
    }
    evidence.peak_merge_inputs = evidence.peak_merge_inputs.max(inputs.len() as u64);
    let mut cursors = Vec::with_capacity(inputs.len());
    let mut counters = Vec::with_capacity(inputs.len());
    let mut schema: Option<SchemaRef> = None;
    for input in inputs {
        let file = root.open_child_file(OsStr::new(input)).map_err(storage)?;
        let counter = IoCounter::default();
        let builder = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
            file,
            counter: counter.clone(),
        })
        .map_err(storage)?;
        if schema
            .as_ref()
            .is_some_and(|known| known.as_ref() != builder.schema().as_ref())
        {
            return Err(storage("row merge schemas differ"));
        }
        schema.get_or_insert_with(|| builder.schema().clone());
        cursors.push(RowCursor {
            reader: Box::new(
                builder
                    .with_batch_size(output_rows.min(4096))
                    .build()
                    .map_err(storage)?,
            ),
            batch: None,
            row: 0,
        });
        counters.push(counter);
    }
    let schema = schema.ok_or_else(|| storage("row merge lacks schema"))?;
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::new(file);
    let buffered = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut writer = ArrowWriter::try_new(buffered, schema.clone(), None).map_err(storage)?;
    let mut heap = BinaryHeap::new();
    for (source, cursor) in cursors.iter_mut().enumerate() {
        if let Some(uuid) = cursor.advance()? {
            heap.push((Reverse(uuid), Reverse(source)));
        }
    }
    let mut selected = Vec::with_capacity(output_rows);
    let mut selected_bytes = 0_usize;
    let mut previous = None;
    while let Some((Reverse(uuid), Reverse(source))) = heap.pop() {
        reject_cancelled(cancelled)?;
        if previous.is_some_and(|prior| prior >= uuid) {
            return Err(storage("duplicate or unordered UUID in row merge"));
        }
        previous = Some(uuid);
        let cursor = &mut cursors[source];
        let batch = cursor
            .batch
            .as_ref()
            .ok_or_else(|| storage("row cursor lacks batch"))?;
        let row_bytes = batch.columns().iter().try_fold(0_usize, |total, column| {
            column
                .slice(cursor.row, 1)
                .to_data()
                .get_slice_memory_size()
                .map(|bytes| total.saturating_add(bytes))
                .map_err(storage)
        })?;
        if row_bytes > output_bytes {
            return Err(storage("one normalized row exceeds merge byte window"));
        }
        if !selected.is_empty()
            && (selected.len() == output_rows
                || selected_bytes.saturating_add(row_bytes) > output_bytes)
        {
            let output_batch = materialize_selected_rows(schema.clone(), &selected)?;
            evidence.merge_read_records = evidence
                .merge_read_records
                .saturating_add(output_batch.num_rows() as u64);
            writer.write(&output_batch).map_err(storage)?;
            evidence.merge_written_records = evidence
                .merge_written_records
                .saturating_add(output_batch.num_rows() as u64);
            selected.clear();
            selected_bytes = 0;
        }
        selected.push((batch.clone(), cursor.row));
        selected_bytes = selected_bytes.saturating_add(row_bytes);
        cursor.row = cursor.row.saturating_add(1);
        if let Some(next) = cursor.advance()? {
            heap.push((Reverse(next), Reverse(source)));
        }
        if heap.is_empty() {
            let batch = materialize_selected_rows(schema.clone(), &selected)?;
            evidence.merge_read_records = evidence
                .merge_read_records
                .saturating_add(batch.num_rows() as u64);
            writer.write(&batch).map_err(storage)?;
            evidence.merge_written_records = evidence
                .merge_written_records
                .saturating_add(batch.num_rows() as u64);
            selected.clear();
            selected_bytes = 0;
        }
    }
    writer.finish().map_err(storage)?;
    writer.sync().map_err(storage)?;
    let hashing = writer.inner().get_ref();
    hashing.inner.sync_all().map_err(storage)?;
    let receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes: hashing.bytes,
        sha256: hex(&hashing.digest.clone().finalize()),
        identity: identity.into(),
        write_operations: hashing.operations,
        fsync_operations: 4,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint("shape.row_merge.after_install");
    persist_shape_receipt(root, &receipt)?;
    evidence.merge_fsync_operations = evidence.merge_fsync_operations.saturating_add(4);
    evidence.merge_groups = evidence.merge_groups.saturating_add(1);
    evidence.merge_written_bytes = evidence.merge_written_bytes.saturating_add(receipt.bytes);
    evidence.parquet_write_bytes = evidence.parquet_write_bytes.saturating_add(receipt.bytes);
    evidence.parquet_write_operations = evidence
        .parquet_write_operations
        .saturating_add(receipt.write_operations);
    for counter in counters {
        counter.add_to(evidence);
    }
    evidence.peak_merge_temporary_bytes = evidence
        .peak_merge_temporary_bytes
        .max(measured_shape_bytes(root)?);
    Ok(receipt)
}

struct RowMergeAccumulator {
    fan_in: usize,
    namespace: String,
    levels: Vec<Vec<String>>,
    groups: Vec<usize>,
    inputs: u64,
}

impl RowMergeAccumulator {
    fn new(fan_in: usize, authority: &str) -> Self {
        Self {
            fan_in,
            namespace: sha256(authority.as_bytes())[..16].to_owned(),
            levels: Vec::new(),
            groups: Vec::new(),
            inputs: 0,
        }
    }

    fn slot_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        root: &StableDirectory,
        mut name: String,
        output_rows: usize,
        output_bytes: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        self.inputs = self.inputs.saturating_add(1);
        let mut level = 0;
        loop {
            if self.levels.len() <= level {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level].push(name);
            if self.levels[level].len() < self.fan_in {
                return Ok(());
            }
            let inputs = std::mem::take(&mut self.levels[level]);
            let group = self.groups[level];
            self.groups[level] = group.saturating_add(1);
            evidence.merge_passes = evidence.merge_passes.max(level as u64 + 1);
            name = format!(
                "merge-rows-{}-l{level:03}-g{group:020}.parquet",
                self.namespace
            );
            merge_row_group(
                root,
                &inputs,
                &name,
                output_rows,
                output_bytes,
                cancelled,
                evidence,
            )?;
            for input in inputs {
                if input.starts_with("merge-rows-") {
                    unlink_named(root, &input)?;
                }
            }
            level += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish(
        mut self,
        root: &StableDirectory,
        output: &str,
        output_rows: usize,
        output_bytes: usize,
        _fan_in: usize,
        cancelled: &mut impl FnMut() -> bool,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<ArtifactReceipt, GfError> {
        if self.inputs == 0 {
            return Err(storage("row merge has no input"));
        }
        let mut level = 0;
        loop {
            let inputs = std::mem::take(
                self.levels
                    .get_mut(level)
                    .ok_or_else(|| storage("row merge scheduler lost its root"))?,
            );
            if inputs.is_empty() {
                level += 1;
                continue;
            }
            let higher_empty = self.levels[level + 1..].iter().all(Vec::is_empty);
            if higher_empty {
                let receipt = merge_row_group(
                    root,
                    &inputs,
                    output,
                    output_rows,
                    output_bytes,
                    cancelled,
                    evidence,
                )?;
                for input in inputs {
                    if input.starts_with("merge-rows-") {
                        unlink_named(root, &input)?;
                    }
                }
                return Ok(receipt);
            }
            let name = if inputs.len() == 1 {
                inputs[0].clone()
            } else {
                let group = self.groups[level];
                self.groups[level] = group.saturating_add(1);
                evidence.merge_passes = evidence.merge_passes.max(level as u64 + 1);
                let output_name = format!(
                    "merge-rows-{}-l{level:03}-g{group:020}.parquet",
                    self.namespace
                );
                merge_row_group(
                    root,
                    &inputs,
                    &output_name,
                    output_rows,
                    output_bytes,
                    cancelled,
                    evidence,
                )?;
                for input in inputs {
                    if input.starts_with("merge-rows-") {
                        unlink_named(root, &input)?;
                    }
                }
                output_name
            };
            if self.levels.len() <= level + 1 {
                self.levels.push(Vec::with_capacity(self.fan_in));
                self.groups.push(0);
            }
            self.levels[level + 1].push(name);
            level += 1;
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One bounded streamed catalog pass; admission must surround each intern.
fn build_runtime_catalog(
    mut catalog: RuntimeCatalog,
    root: &StableDirectory,
    node_rows: &[String],
    edge_rows: &[String],
    now_micros: i64,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<String, GfError> {
    let mut catalog_entries = catalog.entry_count();
    let mut identifier_bytes = catalog.retained_identifier_bytes();
    if catalog_entries > budgets.max_catalog_entries
        || identifier_bytes > budgets.max_catalog_identifier_bytes
    {
        return Err(storage(
            "runtime catalog exceeds construction admission budget",
        ));
    }
    evidence.peak_catalog_entries = evidence.peak_catalog_entries.max(catalog_entries as u64);
    evidence.peak_catalog_identifier_bytes = evidence
        .peak_catalog_identifier_bytes
        .max(identifier_bytes as u64);
    for (kind, names) in [
        (ConstructionChunkKind::Node, node_rows),
        (ConstructionChunkKind::Edge, edge_rows),
    ] {
        for name in names {
            let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
            let counter = IoCounter::default();
            let reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
                file,
                counter: counter.clone(),
            })
            .map_err(storage)?
            .with_batch_size(4096)
            .build()
            .map_err(storage)?;
            for batch in reader {
                reject_cancelled(cancelled)?;
                let batch = batch.map_err(storage)?;
                let decoded_bytes = batch.get_array_memory_size();
                if decoded_bytes > budgets.max_catalog_decoded_bytes {
                    return Err(storage("runtime catalog decoded batch budget exhausted"));
                }
                evidence.peak_catalog_decoded_batch_bytes = evidence
                    .peak_catalog_decoded_batch_bytes
                    .max(decoded_bytes as u64);
                let owner = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| storage("catalog owner column is not Utf8"))?;
                let required = if kind == ConstructionChunkKind::Node {
                    2
                } else {
                    4
                };
                for row in 0..batch.num_rows() {
                    let owner_name = owner.value(row);
                    match kind {
                        ConstructionChunkKind::Node => {
                            admit_catalog_identifier(
                                !catalog.contains_entity_type(owner_name),
                                owner_name.len(),
                                &mut catalog_entries,
                                &mut identifier_bytes,
                                budgets,
                                evidence,
                            )?;
                            catalog.intern_label_at(owner_name, now_micros);
                        }
                        ConstructionChunkKind::Edge => {
                            admit_catalog_identifier(
                                !catalog.contains_relation_type(owner_name),
                                owner_name.len(),
                                &mut catalog_entries,
                                &mut identifier_bytes,
                                budgets,
                                evidence,
                            )?;
                            catalog.intern_relation_type_at(owner_name, now_micros);
                        }
                    }
                    for (offset, field) in batch.schema().fields()[required..].iter().enumerate() {
                        if !batch.column(required + offset).is_null(row) {
                            admit_catalog_identifier(
                                !catalog.contains_property(field.name(), Some(owner_name)),
                                field.name().len().saturating_add(owner_name.len()),
                                &mut catalog_entries,
                                &mut identifier_bytes,
                                budgets,
                                evidence,
                            )?;
                            catalog.intern_property_at(field.name(), Some(owner_name), now_micros);
                        }
                    }
                }
            }
            counter.add_to(evidence);
        }
    }
    let output = "shaped-runtime-catalog.parquet";
    let receipt = write_parquet(root, output, &catalog.to_record_batch())?;
    evidence.merge_fsync_operations = evidence
        .merge_fsync_operations
        .saturating_add(receipt.fsync_operations);
    evidence.parquet_write_bytes = evidence.parquet_write_bytes.saturating_add(receipt.bytes);
    evidence.parquet_write_operations = evidence
        .parquet_write_operations
        .saturating_add(receipt.write_operations);
    account_sequential_write(receipt.bytes, evidence);
    Ok(output.to_owned())
}

#[allow(clippy::too_many_arguments)]
fn admit_catalog_identifier(
    is_new: bool,
    added_identifier_bytes: usize,
    entries: &mut usize,
    identifier_bytes: &mut usize,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if !is_new {
        return Ok(());
    }
    let next_entries = entries
        .checked_add(1)
        .ok_or_else(|| storage("runtime catalog entry count overflow"))?;
    let next_identifier_bytes = identifier_bytes
        .checked_add(added_identifier_bytes)
        .ok_or_else(|| storage("runtime catalog identifier byte count overflow"))?;
    if next_entries > budgets.max_catalog_entries
        || next_identifier_bytes > budgets.max_catalog_identifier_bytes
    {
        return Err(storage("runtime catalog admission budget exhausted"));
    }
    *entries = next_entries;
    *identifier_bytes = next_identifier_bytes;
    evidence.peak_catalog_entries = evidence.peak_catalog_entries.max(next_entries as u64);
    evidence.peak_catalog_identifier_bytes = evidence
        .peak_catalog_identifier_bytes
        .max(next_identifier_bytes as u64);
    Ok(())
}

fn load_parent_runtime_catalog(
    project: &StableDirectory,
    parent_generation: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(RuntimeCatalog, Option<String>, ReadWork), GfError> {
    if parent_generation == 0 {
        return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
    }
    let topology = match project.open_child_directory(OsStr::new("topology")) {
        Ok(topology) => topology,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
        }
        Err(error) => return Err(storage(error)),
    };
    let mut file = match topology.open_child_file(OsStr::new("runtime_catalog.parquet")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
        }
        Err(error) => return Err(storage(error)),
    };
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("parent runtime catalog has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let mut digest = Sha256::new();
    let mut work = ReadWork::default();
    let mut block = vec![0_u8; BLOCK_BYTES];
    loop {
        let count = file.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
        work.bytes = work.bytes.saturating_add(count as u64);
        work.operations = work.operations.saturating_add(1);
    }
    file.rewind().map_err(storage)?;
    if file_identity(&file).map_err(storage)? != identity {
        return Err(storage("parent runtime catalog identity changed"));
    }
    let counter = IoCounter::default();
    let reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file,
        counter: counter.clone(),
    })
    .map_err(storage)?
    .with_batch_size(4_096.min(budgets.max_catalog_entries))
    .build()
    .map_err(storage)?;
    let mut catalog = RuntimeCatalog::new();
    let mut entries = 0_usize;
    let mut decoded_bytes = 0_usize;
    for batch in reader {
        let batch = batch.map_err(storage)?;
        entries = entries
            .checked_add(batch.num_rows())
            .ok_or_else(|| storage("parent runtime catalog entry count overflow"))?;
        decoded_bytes = decoded_bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or_else(|| storage("parent runtime catalog decoded size overflow"))?;
        if entries > budgets.max_catalog_entries
            || decoded_bytes > budgets.max_catalog_decoded_bytes
        {
            return Err(storage("parent runtime catalog admission budget exhausted"));
        }
        catalog.extend_from_record_batch(&batch).map_err(storage)?;
        if catalog.retained_identifier_bytes() > budgets.max_catalog_identifier_bytes {
            return Err(storage(
                "parent runtime catalog identifier budget exhausted",
            ));
        }
    }
    work.bytes = work
        .bytes
        .saturating_add(counter.bytes.load(Ordering::Relaxed));
    work.operations = work
        .operations
        .saturating_add(counter.operations.load(Ordering::Relaxed));
    let named = topology
        .open_child_file(OsStr::new("runtime_catalog.parquet"))
        .map_err(storage)?;
    if file_identity(&named).map_err(storage)? != identity
        || file_link_count(&named).map_err(storage)? != 1
    {
        return Err(storage("parent runtime catalog authority changed"));
    }
    topology.revalidate_named().map_err(storage)?;
    project.revalidate_named().map_err(storage)?;
    Ok((catalog, Some(hex(&digest.finalize())), work))
}

fn load_parent_runtime_catalog_from_compact(
    container_root: &Path,
    inventory: &crate::GraphFilesInventory,
    parent_generation: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(RuntimeCatalog, Option<String>, ReadWork), GfError> {
    if parent_generation == 0 {
        return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
    }
    let Some(entry) = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/runtime_catalog.parquet")
    else {
        return Ok((RuntimeCatalog::new(), None, ReadWork::default()));
    };
    let file = crate::graph_object_store::open_graph_object_by_digest(
        container_root,
        &entry.content_sha256,
        entry.byte_length,
    )?;
    decode_parent_runtime_catalog_file(file, &entry.content_sha256, budgets)
}

fn decode_parent_runtime_catalog_file<R>(
    file: R,
    digest: &str,
    budgets: GraphConstructionBudgets,
) -> Result<(RuntimeCatalog, Option<String>, ReadWork), GfError>
where
    R: ConstructionFileHandle + 'static,
{
    let counter = IoCounter::default();
    let reader = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file,
        counter: counter.clone(),
    })
    .map_err(storage)?
    .with_batch_size(4_096.min(budgets.max_catalog_entries))
    .build()
    .map_err(storage)?;
    let mut catalog = RuntimeCatalog::new();
    let mut entries = 0_usize;
    let mut decoded_bytes = 0_usize;
    for batch in reader {
        let batch = batch.map_err(storage)?;
        entries = entries
            .checked_add(batch.num_rows())
            .ok_or_else(|| storage("parent runtime catalog entry count overflow"))?;
        decoded_bytes = decoded_bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or_else(|| storage("parent runtime catalog decoded size overflow"))?;
        if entries > budgets.max_catalog_entries
            || decoded_bytes > budgets.max_catalog_decoded_bytes
        {
            return Err(storage("parent runtime catalog admission budget exhausted"));
        }
        catalog.extend_from_record_batch(&batch).map_err(storage)?;
        if catalog.retained_identifier_bytes() > budgets.max_catalog_identifier_bytes {
            return Err(storage(
                "parent runtime catalog identifier budget exhausted",
            ));
        }
    }
    let bytes = counter.bytes.load(Ordering::Relaxed);
    let operations = counter.operations.load(Ordering::Relaxed);
    if !is_canonical_sha256(digest) {
        return Err(storage("parent runtime catalog CAS authority changed"));
    }
    Ok((
        catalog,
        Some(digest.to_owned()),
        ReadWork { bytes, operations },
    ))
}

fn read_fixed<const N: usize>(reader: &mut impl Read) -> Result<Option<[u8; N]>, GfError> {
    let mut record = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        match reader.read(&mut record[filled..]).map_err(storage)? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(storage("truncated fixed-width construction run")),
            count => filled += count,
        }
    }
    Ok(Some(record))
}

fn validate_staged_details(
    root: &StableDirectory,
    identities_name: &str,
    node_details_name: Option<&str>,
    edge_details_name: Option<&str>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(u64, u64), GfError> {
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    account_sequential_read(
        identities.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    let mut nodes = 0_u64;
    let mut edges = 0_u64;
    while let Some(record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)? {
        if record[17] != 0 || record[18..].iter().any(|byte| *byte != 0) {
            return Err(storage("staged identity record is not canonical"));
        }
        match record[16] {
            0 => nodes = nodes.saturating_add(1),
            1 => edges = edges.saturating_add(1),
            _ => return Err(storage("invalid staged identity kind")),
        }
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        if nodes.saturating_add(edges).is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    let count = |name: Option<&str>, width: u64| -> Result<u64, GfError> {
        let Some(name) = name else { return Ok(0) };
        let bytes = root
            .open_child_file(OsStr::new(name))
            .map_err(storage)?
            .metadata()
            .map_err(storage)?
            .len();
        if bytes % width != 0 {
            return Err(storage("truncated canonical detail run"));
        }
        Ok(bytes / width)
    };
    if count(node_details_name, NODE_DETAIL_WIDTH as u64)? != nodes
        || count(edge_details_name, EDGE_DETAIL_WIDTH as u64)? != edges
    {
        return Err(storage("staged identity and detail domains disagree"));
    }
    validate_detail_domain::<NODE_DETAIL_WIDTH>(
        root,
        identities_name,
        node_details_name,
        0,
        cancelled,
        evidence,
    )?;
    validate_detail_domain::<EDGE_DETAIL_WIDTH>(
        root,
        identities_name,
        edge_details_name,
        1,
        cancelled,
        evidence,
    )?;
    Ok((nodes, edges))
}

fn reject_staged_base_conflicts(
    root: &StableDirectory,
    identities_name: &str,
    base: &mut AuthenticatedUuidIndexSnapshot,
    window_rows: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    loop {
        let mut requested = Vec::with_capacity(window_rows);
        for _ in 0..window_rows {
            let Some(record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut reader)? else {
                break;
            };
            requested.push(Uuid::from_bytes(
                record[..16].try_into().expect("fixed UUID"),
            ));
        }
        if requested.is_empty() {
            break;
        }
        let (nodes, node_work) = base.probe(UuidIndexKind::Node, &requested)?;
        let (edges, edge_work) = base.probe(UuidIndexKind::Edge, &requested)?;
        account_probe_work(&node_work, evidence);
        account_probe_work(&edge_work, evidence);
        if nodes
            .into_iter()
            .zip(edges)
            .any(|(node, edge)| node || edge)
        {
            return Err(storage("staged UUID conflicts with pinned base identity"));
        }
        reject_cancelled(cancelled)?;
    }
    base.revalidate()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
#[allow(dead_code)]
fn validate_unified_and_details(
    root: &StableDirectory,
    identities_name: &str,
    node_details_name: Option<&str>,
    edge_details_name: Option<&str>,
    endpoints_name: Option<&str>,
    base_max_node: u64,
    base_max_edge: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(u64, u64, u64, u64), GfError> {
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    account_sequential_read(
        identities.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    let mut node_count = 0_u64;
    let mut edge_count = 0_u64;
    let mut new_nodes = 0_u64;
    let mut new_edges = 0_u64;
    while let Some(record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)? {
        match record[16] {
            0 => {
                node_count += 1;
                if record[17] == 0 {
                    new_nodes += 1;
                }
            }
            1 => {
                edge_count += 1;
                if record[17] == 0 {
                    new_edges += 1;
                }
            }
            _ => return Err(storage("invalid identity kind in unified merge")),
        }
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        if (node_count + edge_count).is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    let detail_count = |name: Option<&str>, width: u64| -> Result<u64, GfError> {
        let Some(name) = name else { return Ok(0) };
        let bytes = root
            .open_child_file(OsStr::new(name))
            .map_err(storage)?
            .metadata()
            .map_err(storage)?
            .len();
        if bytes % width != 0 {
            return Err(storage("truncated canonical detail run"));
        }
        Ok(bytes / width)
    };
    if detail_count(node_details_name, NODE_DETAIL_WIDTH as u64)? != new_nodes
        || detail_count(edge_details_name, EDGE_DETAIL_WIDTH as u64)? != new_edges
    {
        return Err(storage("identity and canonical detail domains disagree"));
    }
    validate_detail_domain::<NODE_DETAIL_WIDTH>(
        root,
        identities_name,
        node_details_name,
        0,
        cancelled,
        evidence,
    )?;
    validate_detail_domain::<EDGE_DETAIL_WIDTH>(
        root,
        identities_name,
        edge_details_name,
        1,
        cancelled,
        evidence,
    )?;
    validate_endpoints(
        root,
        identities_name,
        endpoints_name,
        new_edges,
        cancelled,
        evidence,
    )?;
    let max_node = base_max_node
        .checked_add(new_nodes)
        .ok_or_else(|| storage("node surrogate overflow"))?;
    let max_edge = base_max_edge
        .checked_add(new_edges)
        .ok_or_else(|| storage("edge surrogate overflow"))?;
    Ok((node_count, edge_count, max_node, max_edge))
}

fn validate_detail_domain<const N: usize>(
    root: &StableDirectory,
    identities_name: &str,
    details_name: Option<&str>,
    kind: u8,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let Some(details_name) = details_name else {
        return Ok(());
    };
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    let mut details = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(details_name))
            .map_err(storage)?,
    );
    account_sequential_read(
        identities.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    account_sequential_read(
        details.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    let mut count = 0_u64;
    while let Some(detail) = read_fixed::<N>(&mut details)? {
        while identity
            .as_ref()
            .is_some_and(|item| item[17] == 1 || item[16] != kind || item[..16] < detail[..16])
        {
            identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
            account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        }
        if identity
            .as_ref()
            .is_none_or(|item| item[17] != 0 || item[16] != kind || item[..16] != detail[..16])
        {
            return Err(storage(
                "canonical detail UUID differs from identity domain",
            ));
        }
        account_merge_read::<N>(evidence);
        identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        count += 1;
        if count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    while identity
        .as_ref()
        .is_some_and(|item| item[17] == 1 || item[16] != kind)
    {
        identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    }
    if identity.is_some() {
        return Err(storage(
            "identity domain contains a row without canonical detail",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
fn validate_endpoints(
    root: &StableDirectory,
    identities_name: &str,
    endpoints_name: Option<&str>,
    new_edges: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if new_edges == 0 {
        return if endpoints_name.is_none() {
            Ok(())
        } else {
            Err(storage("endpoint run exists without new edges"))
        };
    }
    let endpoints_name = endpoints_name.ok_or_else(|| storage("new edges lack endpoints"))?;
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    let mut endpoints = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(endpoints_name))
            .map_err(storage)?,
    );
    account_sequential_read(
        identities.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    account_sequential_read(
        endpoints.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    let mut endpoint_count = 0_u64;
    while let Some(endpoint) = read_fixed::<ENDPOINT_WIDTH>(&mut endpoints)? {
        while identity
            .as_ref()
            .is_some_and(|item| item[..16] < endpoint[..16])
        {
            identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
            account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        }
        let Some(node) = identity.as_ref() else {
            return Err(storage("edge endpoint UUID does not exist"));
        };
        if node[..16] != endpoint[..16] || node[16] != 0 {
            return Err(storage("edge endpoint is not a node UUID"));
        }
        if endpoint[32] > 1 || endpoint[33..].iter().any(|byte| *byte != 0) {
            return Err(storage("endpoint run record is not canonical"));
        }
        endpoint_count += 1;
        account_merge_read::<ENDPOINT_WIDTH>(evidence);
        if endpoint_count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    if endpoint_count != new_edges.saturating_mul(2) {
        return Err(storage(
            "edge endpoint cardinality differs from edge domain",
        ));
    }
    Ok(())
}

fn assign_surrogates(
    root: &StableDirectory,
    input_name: &str,
    mut node_tail: u64,
    mut edge_tail: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<String, GfError> {
    let output = "shaped-identities.run";
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(input_name))
            .map_err(storage)?,
    );
    account_sequential_read(
        reader.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    let hashing = HashingWriter::new(file);
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut count = 0_u64;
    while let Some(mut record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut reader)? {
        if record[17] == 0 {
            let surrogate = match record[16] {
                0 => {
                    node_tail = node_tail
                        .checked_add(1)
                        .ok_or_else(|| storage("node surrogate overflow"))?;
                    node_tail
                }
                1 => {
                    edge_tail = edge_tail
                        .checked_add(1)
                        .ok_or_else(|| storage("edge surrogate overflow"))?;
                    edge_tail
                }
                _ => return Err(storage("invalid identity kind during shaping")),
            };
            record[24..32].copy_from_slice(&surrogate.to_be_bytes());
        } else if record[17] != 1 {
            return Err(storage("invalid retained identity marker during shaping"));
        }
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
        account_merge_write::<BASE_IDENTITY_WIDTH>(evidence);
        count += 1;
        if count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    writer.flush().map_err(storage)?;
    writer.get_ref().inner.sync_all().map_err(storage)?;
    account_sequential_write(writer.get_ref().bytes, evidence);
    let output_receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes: writer.get_ref().bytes,
        sha256: hex(&writer.get_ref().digest.clone().finalize()),
        identity: identity.into(),
        write_operations: writer.get_ref().operations,
        fsync_operations: 2,
    };
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    persist_shape_receipt(root, &output_receipt)?;
    evidence.merge_fsync_operations = evidence.merge_fsync_operations.saturating_add(2);
    Ok(output.to_owned())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn resolve_endpoint_surrogates(
    root: &StableDirectory,
    identities_name: &str,
    endpoints_name: Option<&str>,
    mut base: Option<&mut AuthenticatedUuidIndexSnapshot>,
    window_rows: usize,
    fan_in: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<Option<String>, GfError> {
    let Some(endpoints_name) = endpoints_name else {
        return Ok(None);
    };
    let mut identities = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(identities_name))
            .map_err(storage)?,
    );
    let mut endpoints = BufReader::with_capacity(
        BLOCK_BYTES,
        root.open_child_file(OsStr::new(endpoints_name))
            .map_err(storage)?,
    );
    account_sequential_read(
        identities.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    account_sequential_read(
        endpoints.get_ref().metadata().map_err(storage)?.len(),
        evidence,
    );
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    let mut window = Vec::<[u8; RESOLVED_ENDPOINT_WIDTH]>::with_capacity(window_rows);
    let mut resolved = FixedMergeAccumulator::new("merge-resolved", fan_in, false);
    let mut sequence = 0_u64;
    loop {
        let mut endpoint_window = Vec::with_capacity(window_rows);
        for _ in 0..window_rows {
            let Some(endpoint) = read_fixed::<ENDPOINT_WIDTH>(&mut endpoints)? else {
                break;
            };
            endpoint_window.push(endpoint);
        }
        if endpoint_window.is_empty() {
            break;
        }
        let mut surrogates = Vec::with_capacity(endpoint_window.len());
        let mut base_requests = Vec::new();
        let mut base_positions = Vec::new();
        for endpoint in &endpoint_window {
            while identity
                .as_ref()
                .is_some_and(|record| record[..16] < endpoint[..16])
            {
                identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
                account_merge_read::<BASE_IDENTITY_WIDTH>(evidence);
            }
            if let Some(node) = identity
                .as_ref()
                .filter(|record| record[..16] == endpoint[..16] && record[16] == 0)
            {
                surrogates.push(Some(u64::from_be_bytes(
                    node[24..32].try_into().expect("fixed"),
                )));
            } else {
                base_positions.push(surrogates.len());
                base_requests.push(Uuid::from_bytes(endpoint[..16].try_into().expect("fixed")));
                surrogates.push(None);
            }
        }
        if !base_requests.is_empty() {
            let (resolved, probe_work) = base
                .as_deref_mut()
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?
                .lookup_node_surrogates(&base_requests)?;
            account_probe_work(&probe_work, evidence);
            for (position, surrogate) in base_positions.into_iter().zip(resolved) {
                surrogates[position] = surrogate;
            }
        }
        for (endpoint, surrogate) in endpoint_window.into_iter().zip(surrogates) {
            let surrogate = surrogate
                .filter(|value| *value != 0)
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?;
            let mut resolved = [0_u8; RESOLVED_ENDPOINT_WIDTH];
            resolved[..16].copy_from_slice(&endpoint[16..32]);
            resolved[16] = endpoint[32];
            resolved[24..32].copy_from_slice(&surrogate.to_be_bytes());
            window.push(resolved);
            account_merge_read::<ENDPOINT_WIDTH>(evidence);
        }
        if window.len() == window_rows {
            window.sort_unstable();
            let name = format!("merge-resolved-source-{sequence:020}.run");
            let receipt = write_fixed_run(root, &name, &window)?;
            evidence.merge_written_bytes =
                evidence.merge_written_bytes.saturating_add(receipt.bytes);
            evidence.merge_written_records = evidence
                .merge_written_records
                .saturating_add(window.len() as u64);
            evidence.merge_fsync_operations = evidence
                .merge_fsync_operations
                .saturating_add(receipt.fsync_operations);
            account_sequential_write(receipt.bytes, evidence);
            resolved.push::<RESOLVED_ENDPOINT_WIDTH>(root, name, cancelled, evidence)?;
            evidence.peak_resolved_endpoint_name_slots = evidence
                .peak_resolved_endpoint_name_slots
                .max(resolved.slot_count() as u64);
            window.clear();
            sequence = sequence.saturating_add(1);
            reject_cancelled(cancelled)?;
        }
    }
    if !window.is_empty() {
        window.sort_unstable();
        let name = format!("merge-resolved-source-{sequence:020}.run");
        let receipt = write_fixed_run(root, &name, &window)?;
        evidence.merge_written_bytes = evidence.merge_written_bytes.saturating_add(receipt.bytes);
        evidence.merge_written_records = evidence
            .merge_written_records
            .saturating_add(window.len() as u64);
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .saturating_add(receipt.fsync_operations);
        account_sequential_write(receipt.bytes, evidence);
        resolved.push::<RESOLVED_ENDPOINT_WIDTH>(root, name, cancelled, evidence)?;
        evidence.peak_resolved_endpoint_name_slots = evidence
            .peak_resolved_endpoint_name_slots
            .max(resolved.slot_count() as u64);
    }
    resolved.finish_optional::<RESOLVED_ENDPOINT_WIDTH>(root, cancelled, evidence)
}

fn account_probe_work(
    work: &crate::uuid_membership::UuidProbeMetrics,
    evidence: &mut GraphConstructionEvidence,
) {
    evidence.retained_probe_read_bytes = evidence
        .retained_probe_read_bytes
        .saturating_add(work.identity_bytes_read)
        .saturating_add(work.surrogate_bytes_read);
    evidence.retained_probe_block_loads = evidence
        .retained_probe_block_loads
        .saturating_add(work.identity_blocks_read)
        .saturating_add(work.surrogate_blocks_read);
}

#[derive(Clone, Copy, Debug, Default)]
struct ReadWork {
    bytes: u64,
    operations: u64,
}

fn authenticate_artifact(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
) -> Result<ReadWork, GfError> {
    validate_artifact_name(receipt)?;
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    if !receipt
        .identity
        .matches(file_identity(&file).map_err(storage)?)
        || file_link_count(&file).map_err(storage)? != 1
    {
        return Err(storage("artifact identity or link count changed"));
    }
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, file);
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut operations = 0_u64;
    let width = if receipt.name.ends_with(".identities.run") {
        Some(IDENTITY_WIDTH)
    } else if receipt.name.ends_with(".endpoints.run") {
        Some(ENDPOINT_WIDTH)
    } else if receipt.name.ends_with(".node-details.run") {
        Some(NODE_DETAIL_WIDTH)
    } else if receipt.name.ends_with(".edge-details.run") {
        Some(EDGE_DETAIL_WIDTH)
    } else {
        None
    };
    let mut pending = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    loop {
        let count = reader.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
        bytes = bytes.saturating_add(count as u64);
        operations = operations.saturating_add(1);
        if let Some(width) = width {
            pending.extend_from_slice(&block[..count]);
            let complete = pending.len() / width * width;
            for record in pending[..complete].chunks_exact(width) {
                if previous
                    .as_ref()
                    .is_some_and(|prior| prior.as_slice() >= record)
                {
                    return Err(storage("fixed construction run is not strictly sorted"));
                }
                if width == ENDPOINT_WIDTH
                    && (!matches!(record[32], 0 | 1) || record[33..].iter().any(|byte| *byte != 0))
                {
                    return Err(storage("endpoint run record is malformed"));
                }
                if width == EDGE_DETAIL_WIDTH {
                    let route_len = record[48] as usize;
                    if route_len == 0 || record[49 + route_len..].iter().any(|byte| *byte != 0) {
                        return Err(storage("edge detail run record is malformed"));
                    }
                }
                if width == NODE_DETAIL_WIDTH {
                    let label_len = record[16] as usize;
                    if label_len == 0 || record[17 + label_len..].iter().any(|byte| *byte != 0) {
                        return Err(storage("node detail run record is malformed"));
                    }
                }
                if width == BASE_IDENTITY_WIDTH
                    && (!matches!(record[16], 0 | 1)
                        || record[17] != 1
                        || record[18..24].iter().any(|byte| *byte != 0)
                        || (record[16] == 0 && record[24..].iter().all(|byte| *byte == 0))
                        || (record[16] == 1 && record[24..].iter().any(|byte| *byte != 0)))
                {
                    return Err(storage("base identity run record is malformed"));
                }
                previous = Some(record.to_vec());
            }
            pending.drain(..complete);
        }
    }
    if !pending.is_empty() {
        return Err(storage("fixed construction run has a truncated tail"));
    }
    if bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
        return Err(storage("artifact digest or size changed"));
    }
    Ok(ReadWork { bytes, operations })
}

fn validate_artifact_name(receipt: &ArtifactReceipt) -> Result<(), GfError> {
    let valid_suffix = receipt.name.ends_with(".parquet")
        || receipt.name.ends_with(".identities.run")
        || receipt.name.ends_with(".endpoints.run")
        || receipt.name.ends_with(".node-details.run")
        || receipt.name.ends_with(".edge-details.run");
    if !valid_suffix
        || receipt.name.starts_with('.')
        || receipt.name.contains('/')
        || receipt.name.contains('\\')
        || !is_canonical_sha256(&receipt.sha256)
        || !is_canonical_lower_hex(&receipt.identity.file_id, 32)
        || receipt.bytes == 0
        || receipt.write_operations == 0
        || receipt.fsync_operations == 0
    {
        return Err(storage("invalid construction artifact receipt"));
    }
    if receipt.name.ends_with(".identities.run")
        && !receipt.bytes.is_multiple_of(IDENTITY_WIDTH as u64)
    {
        return Err(storage("truncated identity run"));
    }
    if receipt.name.ends_with(".endpoints.run")
        && !receipt.bytes.is_multiple_of(ENDPOINT_WIDTH as u64)
    {
        return Err(storage("truncated endpoint run"));
    }
    if receipt.name.ends_with(".node-details.run")
        && !receipt.bytes.is_multiple_of(NODE_DETAIL_WIDTH as u64)
    {
        return Err(storage("truncated node detail run"));
    }
    if receipt.name.ends_with(".edge-details.run")
        && !receipt.bytes.is_multiple_of(EDGE_DETAIL_WIDTH as u64)
    {
        return Err(storage("truncated edge detail run"));
    }
    Ok(())
}

fn receipt_from_intent(intent: &ChunkIntent) -> Result<ConstructionChunkReceipt, GfError> {
    Ok(ConstructionChunkReceipt {
        operation_uuid: intent.operation_uuid,
        project_identity: intent.project_identity.clone(),
        session_identity: intent.session_identity.clone(),
        parent_topology_generation: intent.parent_topology_generation,
        ontology_mode: intent.ontology_mode,
        semantic_authority_sha256: intent.semantic_authority_sha256.clone(),
        prior_receipt_sha256: intent.prior_receipt_sha256.clone(),
        chunk_id: intent.chunk_id.clone(),
        sequence: intent.sequence,
        kind: intent.kind,
        rows: intent.rows,
        input_bytes: intent.input_bytes,
        input_sha256: intent.input_sha256.clone(),
        schema_sha256: intent.schema_sha256.clone(),
        run_records: intent.run_records,
        accounted_live_bytes: intent.accounted_live_bytes,
        parquet: intent
            .parquet
            .clone()
            .ok_or_else(|| storage("intent lacks Parquet artifact"))?,
        identities: intent
            .identities
            .clone()
            .ok_or_else(|| storage("intent lacks identity run"))?,
        endpoints: intent.endpoints.clone(),
        details: intent
            .details
            .clone()
            .ok_or_else(|| storage("intent lacks detail run"))?,
    })
}

fn validate_receipt_semantics(
    receipt: &ConstructionChunkReceipt,
    sequence: u64,
    budgets: GraphConstructionBudgets,
) -> Result<(), GfError> {
    validate_chunk_id(&receipt.chunk_id)?;
    if receipt.sequence != sequence
        || receipt.operation_uuid.is_nil()
        || receipt.rows == 0
        || receipt.rows > budgets.max_batch_rows as u64
        || receipt.input_bytes > budgets.max_batch_bytes as u64
        || receipt.run_records > budgets.max_run_records as u64
        || receipt.accounted_live_bytes == 0
        || !is_canonical_sha256(&receipt.input_sha256)
        || !is_canonical_sha256(&receipt.schema_sha256)
        || receipt.parquet.name != format!("{}.parquet", artifact_stem(sequence, receipt.kind))
        || receipt.identities.name
            != format!("{}.identities.run", artifact_stem(sequence, receipt.kind))
        || receipt.details.name
            != format!(
                "{}.{}-details.run",
                artifact_stem(sequence, receipt.kind),
                if receipt.kind == ConstructionChunkKind::Node {
                    "node"
                } else {
                    "edge"
                }
            )
        || (receipt.kind == ConstructionChunkKind::Node && receipt.endpoints.is_some())
        || (receipt.kind == ConstructionChunkKind::Edge && receipt.endpoints.is_none())
        || receipt.identities.bytes / IDENTITY_WIDTH as u64 != receipt.rows
        || receipt.run_records
            != receipt.rows
                * if receipt.kind == ConstructionChunkKind::Edge {
                    4
                } else {
                    2
                }
    {
        return Err(storage("receipt semantics are inconsistent"));
    }
    if let Some(endpoints) = &receipt.endpoints
        && (endpoints.name != format!("{}.endpoints.run", artifact_stem(sequence, receipt.kind))
            || endpoints.bytes / ENDPOINT_WIDTH as u64 != receipt.rows * 2)
    {
        return Err(storage("endpoint receipt semantics are inconsistent"));
    }
    let detail_width = if receipt.kind == ConstructionChunkKind::Node {
        NODE_DETAIL_WIDTH
    } else {
        EDGE_DETAIL_WIDTH
    };
    if receipt.details.bytes / detail_width as u64 != receipt.rows {
        return Err(storage("detail receipt semantics are inconsistent"));
    }
    validate_artifact_name(&receipt.parquet)?;
    validate_artifact_name(&receipt.identities)?;
    validate_artifact_name(&receipt.details)?;
    if let Some(endpoints) = &receipt.endpoints {
        validate_artifact_name(endpoints)?;
    }
    Ok(())
}

fn validate_receipt_artifacts(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<ReadWork, GfError> {
    let mut work = ReadWork::default();
    for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
        .into_iter()
        .chain(receipt.endpoints.iter())
    {
        let artifact_work = authenticate_artifact(root, artifact)?;
        work.bytes = work.bytes.saturating_add(artifact_work.bytes);
        work.operations = work.operations.saturating_add(artifact_work.operations);
    }
    let parquet_work = validate_parquet_shape(root, receipt)?;
    work.bytes = work.bytes.saturating_add(parquet_work.bytes);
    work.operations = work.operations.saturating_add(parquet_work.operations);
    Ok(work)
}

fn validate_parquet_shape(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.parquet.name))
        .map_err(storage)?;
    if !receipt
        .parquet
        .identity
        .matches(file_identity(&file).map_err(storage)?)
    {
        return Err(storage("Parquet identity changed during schema reopen"));
    }
    let counter = IoCounter::default();
    let builder = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file,
        counter: counter.clone(),
    })
    .map_err(storage)?;
    let expected_prefix = match receipt.kind {
        ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
        ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
    };
    let schema = builder.schema();
    if schema.fields().len() < expected_prefix.fields().len()
        || schema.fields()[..expected_prefix.fields().len()] != expected_prefix.fields()[..]
        || normalized_schema_digest(schema.as_ref()) != receipt.schema_sha256
        || builder.metadata().file_metadata().num_rows()
            != i64::try_from(receipt.rows).map_err(|_| storage("receipt row count exceeds i64"))?
    {
        return Err(storage("Parquet schema or row count differs from receipt"));
    }
    let mut reader = builder.with_batch_size(4096).build().map_err(storage)?;
    let identity_name = if receipt.kind == ConstructionChunkKind::Node {
        "node_uuid"
    } else {
        "edge_uuid"
    };
    let mut previous: Option<[u8; 16]> = None;
    for batch in &mut reader {
        let batch = batch.map_err(storage)?;
        let identities = uuid_column(&batch, identity_name)?;
        for row in 0..batch.num_rows() {
            let value = uuid_value(identities, row)?;
            if previous.is_some_and(|prior| prior >= value) {
                return Err(storage("row artifact is not strictly UUID sorted"));
            }
            previous = Some(value);
        }
    }
    Ok(ReadWork {
        bytes: counter.bytes.load(Ordering::Relaxed),
        operations: counter.operations.load(Ordering::Relaxed),
    })
}

fn validate_parquet_metadata(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.parquet.name))
        .map_err(storage)?;
    if !receipt
        .parquet
        .identity
        .matches(file_identity(&file).map_err(storage)?)
    {
        return Err(storage("Parquet identity changed during metadata reopen"));
    }
    let counter = IoCounter::default();
    let builder = ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file,
        counter: counter.clone(),
    })
    .map_err(storage)?;
    let expected_prefix = match receipt.kind {
        ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
        ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
    };
    let schema = builder.schema();
    if schema.fields().len() < expected_prefix.fields().len()
        || schema.fields()[..expected_prefix.fields().len()] != expected_prefix.fields()[..]
        || normalized_schema_digest(schema.as_ref()) != receipt.schema_sha256
        || builder.metadata().file_metadata().num_rows()
            != i64::try_from(receipt.rows).map_err(|_| storage("receipt row count exceeds i64"))?
    {
        return Err(storage("Parquet schema or row count differs from receipt"));
    }
    Ok(ReadWork {
        bytes: counter.bytes.load(Ordering::Relaxed),
        operations: counter.operations.load(Ordering::Relaxed),
    })
}

fn validate_intent(intent: &ChunkIntent, checkpoint: &Checkpoint) -> Result<(), GfError> {
    let stem = artifact_stem(intent.sequence, intent.kind);
    let expected_run_records =
        intent
            .rows
            .saturating_mul(if intent.kind == ConstructionChunkKind::Edge {
                4
            } else {
                2
            });
    if intent.format_version != FORMAT_VERSION
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || !(intent.sequence == checkpoint.next_sequence
            || intent.sequence.saturating_add(1) == checkpoint.next_sequence)
        || intent.parent_topology_generation != checkpoint.parent_topology_generation
        || intent.ontology_mode != checkpoint.ontology_mode
        || intent.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
        || (intent.sequence == checkpoint.next_sequence
            && intent.prior_receipt_sha256 != checkpoint.last_receipt_sha256)
        || intent.chunk_key != chunk_key_name(&intent.chunk_id)
        || intent.rows == 0
        || intent.rows > checkpoint.budgets.max_batch_rows as u64
        || intent.input_bytes > checkpoint.budgets.max_batch_bytes as u64
        || intent.run_records > checkpoint.budgets.max_run_records as u64
        || intent.accounted_live_bytes > 0 && intent.accounted_live_bytes < intent.input_bytes
        || intent.run_records != expected_run_records
        || !is_canonical_sha256(&intent.input_sha256)
        || !is_canonical_sha256(&intent.schema_sha256)
        || intent
            .parquet
            .as_ref()
            .is_some_and(|artifact| artifact.name != format!("{stem}.parquet"))
        || intent.identities.as_ref().is_some_and(|artifact| {
            artifact.name != format!("{stem}.identities.run")
                || artifact.bytes != intent.rows.saturating_mul(IDENTITY_WIDTH as u64)
        })
        || intent.endpoints.as_ref().is_some_and(|artifact| {
            intent.kind != ConstructionChunkKind::Edge
                || artifact.name != format!("{stem}.endpoints.run")
                || artifact.bytes
                    != intent
                        .rows
                        .saturating_mul(2)
                        .saturating_mul(ENDPOINT_WIDTH as u64)
        })
        || intent.details.as_ref().is_some_and(|artifact| {
            artifact.name
                != format!(
                    "{stem}.{}-details.run",
                    if intent.kind == ConstructionChunkKind::Node {
                        "node"
                    } else {
                        "edge"
                    }
                )
                || artifact.bytes
                    != intent
                        .rows
                        .saturating_mul(if intent.kind == ConstructionChunkKind::Node {
                            NODE_DETAIL_WIDTH
                        } else {
                            EDGE_DETAIL_WIDTH
                        } as u64)
        })
    {
        return Err(storage("durable intent is inconsistent with checkpoint"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_checkpoint(
    checkpoint: &Checkpoint,
    operation: Uuid,
    project: FileIdentity,
    session: FileIdentity,
    generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    semantic_authority_sha256: Option<&str>,
    budgets: GraphConstructionBudgets,
    parent_catalog_sha256: Option<&str>,
    parent_generation_uuid: Uuid,
    parent_generation_manifest_sha256: &str,
) -> Result<(), GfError> {
    if checkpoint.format_version != FORMAT_VERSION
        || checkpoint.operation_uuid != operation
        || !checkpoint.project_identity.matches(project)
        || !checkpoint.session_identity.matches(session)
        || checkpoint.parent_topology_generation != generation
        || checkpoint.parent_generation_uuid != parent_generation_uuid
        || checkpoint.parent_generation_manifest_sha256 != parent_generation_manifest_sha256
        || checkpoint.parent_generation_uuid.is_nil()
        || validate_sha256(
            &checkpoint.parent_generation_manifest_sha256,
            "parent generation manifest",
        )
        .is_err()
        || checkpoint.ontology_mode != ontology_mode
        || checkpoint.lifecycle_mode != lifecycle_mode
        || checkpoint.semantic_authority_sha256.as_deref() != semantic_authority_sha256
        || checkpoint
            .semantic_authority_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint.session_now_micros <= 0
        || checkpoint.budgets != budgets
        || checkpoint.has_base_snapshot != (generation != 0)
        || checkpoint.parent_catalog_sha256.as_deref() != parent_catalog_sha256
        || checkpoint
            .parent_catalog_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint.next_sequence > budgets.max_chunks
        || checkpoint.last_receipt_sha256.is_some() != (checkpoint.next_sequence != 0)
        || checkpoint
            .last_receipt_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .shape_authority_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .encoding_inventory_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint.encoding_inventory_sha256.is_some()
            && checkpoint.shape_authority_sha256.is_none()
        || match checkpoint.state {
            GraphConstructionState::Staging | GraphConstructionState::Aborted => {
                checkpoint.publication_state.is_some()
            }
            GraphConstructionState::Sealed => checkpoint.publication_state.is_none(),
        }
        || matches!(
            checkpoint.publication_state,
            Some(
                ConstructionPublicationState::Publishing | ConstructionPublicationState::Published
            )
        ) && checkpoint.encoding_inventory_sha256.is_none()
        || checkpoint.evidence.input_batches != checkpoint.next_sequence
        || checkpoint.evidence.parquet_shards != checkpoint.next_sequence
        || checkpoint.evidence.peak_batch_rows > budgets.max_batch_rows as u64
        || checkpoint.evidence.peak_batch_bytes > budgets.max_batch_bytes as u64
        || checkpoint.evidence.peak_run_records > budgets.max_run_records as u64
        || checkpoint.evidence.prior_topology_rows_decoded != 0
        || checkpoint.evidence.current_transitions != 0
        || checkpoint
            .node_schema_sha256
            .iter()
            .any(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .edge_schema_sha256
            .iter()
            .any(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .node_schema_sha256
            .len()
            .saturating_add(checkpoint.edge_schema_sha256.len())
            > budgets.max_schema_groups
    {
        return Err(storage("checkpoint authority or resume parameters changed"));
    }
    Ok(())
}

fn cleanup_authenticated_control_temps(
    root: &StableDirectory,
    operation: Uuid,
    project: FileIdentity,
    session: FileIdentity,
) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !is_control_temp(text) {
            continue;
        }
        let mut file = root.open_child_file(&name).map_err(storage)?;
        let body = read_bounded_limit(&mut file, MAX_SHAPE_CONTROL_BYTES)?;
        let authenticated = serde_json::from_slice::<Checkpoint>(&body).is_ok_and(|value| {
            value.format_version == FORMAT_VERSION
                && value.operation_uuid == operation
                && value.project_identity.matches(project)
                && value.session_identity.matches(session)
        }) || serde_json::from_slice::<ChunkIntent>(&body).is_ok_and(|value| {
            value.format_version == FORMAT_VERSION
                && value.operation_uuid == operation
                && value.project_identity.matches(project)
                && value.session_identity.matches(session)
        }) || serde_json::from_slice::<ConstructionChunkReceipt>(&body)
            .is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ReceiptPointer>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ShapeIntent>(&body).is_ok_and(|value| {
                value.format_version == FORMAT_VERSION
                    && value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ConstructionPublicationIntent>(&body).is_ok_and(|value| {
                value.format_version == FORMAT_VERSION
                    && value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ConstructionPublicationReceipt>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            });
        if authenticated && file_link_count(&file).map_err(storage)? == 1 {
            let identity = file_identity(&file).map_err(storage)?;
            drop(file);
            root.unlink_child_if_identity(&name, identity)
                .map_err(storage)?;
            root.sync().map_err(storage)?;
        }
    }
    Ok(())
}

fn is_control_temp(name: &str) -> bool {
    let Some((prefix, suffix)) = name.rsplit_once('.') else {
        return false;
    };
    suffix == "tmp"
        && prefix.starts_with('.')
        && prefix.rsplit_once('-').is_some_and(|(_, random)| {
            random.len() == 32 && random.bytes().all(|b| b.is_ascii_hexdigit())
        })
}

fn cleanup_owned_artifact_temps(root: &StableDirectory) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !is_owned_artifact_temp(text) {
            continue;
        }
        let file = root.open_child_file(&name).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("incomplete artifact temp has unexpected links"));
        }
        let identity = file_identity(&file).map_err(storage)?;
        drop(file);
        root.unlink_child_if_identity(&name, identity)
            .map_err(storage)?;
        root.sync().map_err(storage)?;
    }
    Ok(())
}

fn is_owned_artifact_temp(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix(".artifact-")
        .and_then(|value| value.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((target, random)) = body.rsplit_once('-') else {
        return false;
    };
    random.len() == 32
        && random.bytes().all(|byte| byte.is_ascii_hexdigit())
        && canonical_artifact_target(target)
}

fn canonical_artifact_target(name: &str) -> bool {
    if is_shape_artifact_name(name) {
        return true;
    }
    let Some(body) = name.strip_prefix("chunk-") else {
        return false;
    };
    let Some((sequence, tail)) = body.split_once('-') else {
        return false;
    };
    sequence.len() == 20
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && matches!(
            tail,
            "node.parquet"
                | "node.identities.run"
                | "edge.parquet"
                | "edge.identities.run"
                | "edge.endpoints.run"
                | "node.node-details.run"
                | "edge.edge-details.run"
        )
}

fn install_control<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
) -> Result<(), GfError> {
    let body = encode_control(value, target)?;
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "install")?;
    file.sync_all().map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_temp_fsync.{target}"));
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_install.{target}"));
    Ok(())
}

fn replace_control<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
) -> Result<(), GfError> {
    let body = encode_control(value, target)?;
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "replace")?;
    file.sync_all().map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.replace.after_temp_fsync.{target}"));
    root.replace_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.replace.after_replace.{target}"));
    Ok(())
}

fn control_limit(target: &str) -> u64 {
    if target == SHAPE_INTENT {
        MAX_SHAPE_CONTROL_BYTES
    } else {
        MAX_CONTROL_BYTES
    }
}

struct BoundedControlWriter {
    body: Vec<u8>,
    limit: usize,
}

impl Write for BoundedControlWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.body.len().saturating_add(bytes.len()) > self.limit {
            return Err(std::io::Error::other("control record exceeds bound"));
        }
        self.body.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_control<T: Serialize>(value: &T, target: &str) -> Result<Vec<u8>, GfError> {
    let limit = usize::try_from(control_limit(target)).map_err(storage)?;
    let mut writer = BoundedControlWriter {
        body: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(storage)?;
    Ok(writer.body)
}

fn decode_bounded<T: for<'de> Deserialize<'de>>(file: &mut File) -> Result<T, GfError> {
    serde_json::from_slice(&read_bounded(file)?).map_err(storage)
}

fn decode_shape_intent(file: &mut File) -> Result<ShapeIntent, GfError> {
    serde_json::from_slice(&read_bounded_limit(file, MAX_SHAPE_CONTROL_BYTES)?).map_err(storage)
}

fn read_bounded(file: &mut File) -> Result<Vec<u8>, GfError> {
    read_bounded_limit(file, MAX_CONTROL_BYTES)
}

fn read_bounded_limit(file: &mut File, limit: u64) -> Result<Vec<u8>, GfError> {
    if file.metadata().map_err(storage)?.len() > limit {
        return Err(storage("control record exceeds bound"));
    }
    let mut body = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut body)
        .map_err(storage)?;
    if body.len() as u64 > limit {
        return Err(storage("control record exceeds bound"));
    }
    Ok(body)
}

fn unlink_named(root: &StableDirectory, name: &str) -> Result<(), GfError> {
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("control record link count changed"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn unlink_artifact(root: &StableDirectory, receipt: &ArtifactReceipt) -> Result<(), GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    if !receipt.identity.matches(identity) || file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("orphan artifact identity changed"));
    }
    unlink_writer_capability(root, &receipt.name, Some(receipt))?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(&receipt.name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn remove_unrecorded_artifact(
    root: &StableDirectory,
    name: &str,
    kind: ConstructionChunkKind,
    rows: u64,
) -> Result<(), GfError> {
    let mut file = match root.open_child_file(OsStr::new(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let identity = file_identity(&file).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("unrecorded artifact has unexpected links"));
    }
    if name.ends_with(".parquet") {
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(storage)?;
        let expected = match kind {
            ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
            ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
        };
        if builder.schema().fields().len() < expected.fields().len()
            || builder.schema().fields()[..expected.fields().len()] != expected.fields()[..]
            || builder.metadata().file_metadata().num_rows()
                != i64::try_from(rows).map_err(|_| storage("artifact row count exceeds i64"))?
        {
            return Err(storage("unrecorded Parquet artifact is not session-owned"));
        }
    } else {
        let width = if name.ends_with(".identities.run") {
            IDENTITY_WIDTH
        } else if name.ends_with(".endpoints.run") {
            ENDPOINT_WIDTH
        } else if name.ends_with(".node-details.run") {
            NODE_DETAIL_WIDTH
        } else if name.ends_with(".edge-details.run") {
            EDGE_DETAIL_WIDTH
        } else {
            return Err(storage("unrecorded artifact name is not canonical"));
        };
        let expected_records = if width == ENDPOINT_WIDTH {
            rows.saturating_mul(2)
        } else {
            rows
        };
        if file.metadata().map_err(storage)?.len() != expected_records.saturating_mul(width as u64)
        {
            return Err(storage("unrecorded fixed run row count changed"));
        }
        validate_sorted_run(&mut file, width)?;
    }
    unlink_writer_capability(root, name, None)?;
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn validate_sorted_run(file: &mut File, width: usize) -> Result<(), GfError> {
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, file);
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut pending = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    loop {
        let count = reader.read(&mut block).map_err(storage)?;
        if count == 0 {
            break;
        }
        pending.extend_from_slice(&block[..count]);
        let complete = pending.len() / width * width;
        for record in pending[..complete].chunks_exact(width) {
            if previous
                .as_ref()
                .is_some_and(|prior| prior.as_slice() >= record)
                || (width == ENDPOINT_WIDTH
                    && (!matches!(record[32], 0 | 1) || record[33..].iter().any(|byte| *byte != 0)))
                || (width == EDGE_DETAIL_WIDTH
                    && (record[48] == 0
                        || record[49 + record[48] as usize..]
                            .iter()
                            .any(|byte| *byte != 0)))
                || (width == NODE_DETAIL_WIDTH
                    && (record[16] == 0
                        || record[17 + record[16] as usize..]
                            .iter()
                            .any(|byte| *byte != 0)))
                || (width == BASE_IDENTITY_WIDTH
                    && (!matches!(record[16], 0 | 1)
                        || record[17] != 1
                        || record[18..24].iter().any(|byte| *byte != 0)
                        || (record[16] == 0 && record[24..].iter().all(|byte| *byte == 0))
                        || (record[16] == 1 && record[24..].iter().any(|byte| *byte != 0))))
            {
                return Err(storage("unrecorded fixed run is malformed"));
            }
            previous = Some(record.to_vec());
        }
        pending.drain(..complete);
    }
    if !pending.is_empty() {
        return Err(storage("unrecorded fixed run has truncated tail"));
    }
    Ok(())
}

fn artifact_stem(sequence: u64, kind: ConstructionChunkKind) -> String {
    format!("chunk-{sequence:020}-{}", kind.tag())
}

fn receipt_name(sequence: u64) -> String {
    format!("receipt-{sequence:020}.json")
}

fn chunk_key_name(chunk_id: &str) -> String {
    format!("key-{}.json", sha256(chunk_id.as_bytes()))
}

fn control_temp(target: &str) -> OsString {
    OsString::from(format!(".{target}-{}.tmp", Uuid::new_v4().simple()))
}

fn artifact_temp(target: &str) -> OsString {
    OsString::from(format!(
        ".artifact-{target}-{}.tmp",
        Uuid::new_v4().simple()
    ))
}

fn validate_chunk_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(storage("invalid chunk id"));
    }
    Ok(())
}

fn reject_cancelled(cancelled: &mut impl FnMut() -> bool) -> Result<(), GfError> {
    if cancelled() {
        return Err(storage("construction cancelled"));
    }
    Ok(())
}

fn write_control_body(
    file: &mut File,
    body: &[u8],
    target: &str,
    operation: &str,
) -> Result<(), GfError> {
    #[cfg(test)]
    {
        let middle = body.len() / 2;
        file.write_all(&body[..middle]).map_err(storage)?;
        file.sync_all().map_err(storage)?;
        construction_failpoint(&format!("control.{operation}.after_partial.{target}"));
        file.write_all(&body[middle..]).map_err(storage)?;
    }
    #[cfg(not(test))]
    {
        let _ = (target, operation);
        file.write_all(body).map_err(storage)?;
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
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryArray, FixedSizeBinaryArray, Int64Array, StringArray};
    use tempfile::TempDir;

    use super::*;

    fn fixed(values: &[[u8; 16]]) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice())).unwrap()
    }

    fn tree_has_no_temps(path: &Path) -> bool {
        std::fs::read_dir(path).unwrap().all(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                tree_has_no_temps(&path)
            } else {
                !entry.file_name().to_string_lossy().ends_with(".tmp")
            }
        })
    }

    #[test]
    fn shaped_writer_capability_resume_adopts_only_the_exact_receipt() {
        let temporary = TempDir::new().unwrap();
        let root = StableDirectory::open(temporary.path()).unwrap();
        std::fs::write(temporary.path().join("shaped-identities.run"), b"identity").unwrap();
        let (receipt, _) = receipt_for_existing_with_work(&root, "shaped-identities.run").unwrap();

        persist_shape_receipt(&root, &receipt).unwrap();
        persist_shape_receipt(&root, &receipt).unwrap();

        let mut mismatched = receipt;
        mismatched.sha256 = "0".repeat(64);
        let error = persist_shape_receipt(&root, &mismatched)
            .unwrap_err()
            .to_string();
        assert!(error.contains("shaped writer capability changed"));
    }

    fn node_batch(first: u128, rows: usize) -> RecordBatch {
        let uuids = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&uuids)),
                Arc::new(StringArray::from(vec!["Person"; rows])),
            ],
        )
        .unwrap()
    }

    fn distinct_label_batch(first: u128, rows: usize) -> RecordBatch {
        let uuids = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let labels = (first..first + rows as u128)
            .map(|index| format!("Label{index:08}"))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![Arc::new(fixed(&uuids)), Arc::new(StringArray::from(labels))],
        )
        .unwrap()
    }

    fn edge_batch(first: u128, rows: usize) -> RecordBatch {
        let edges = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let src = (1..=rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let dst = (2..=rows as u128 + 1)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&edges)),
                Arc::new(StringArray::from(vec!["R"; rows])),
                Arc::new(fixed(&src)),
                Arc::new(fixed(&dst)),
            ],
        )
        .unwrap()
    }

    fn node_property_batch(first: u128, rows: usize) -> RecordBatch {
        node_property_batch_for(first, rows, "Person")
    }

    fn node_property_batch_for(first: u128, rows: usize, label: &str) -> RecordBatch {
        let uuids = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let mut fields = CONSTRUCTION_NODE_SCHEMA
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("score", DataType::Int64, true));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(&uuids)),
                Arc::new(StringArray::from(vec![label; rows])),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row as i64 + 10),
                )),
            ],
        )
        .unwrap()
    }

    fn child_parent_property_batch(first: u128, rows: usize) -> RecordBatch {
        let uuids = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let mut fields = CONSTRUCTION_NODE_SCHEMA
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("score", DataType::Int64, true));
        fields.push(Field::new("nickname", DataType::Utf8, true));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(&uuids)),
                Arc::new(StringArray::from(vec!["Child"; rows])),
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(StringArray::from(vec!["kid"; rows])),
            ],
        )
        .unwrap()
    }

    fn colliding_property_batch(uuid: u128, label: &str, value: i64) -> RecordBatch {
        let mut fields = CONSTRUCTION_NODE_SCHEMA
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("value", DataType::Int64, true));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(&[uuid.to_be_bytes()])),
                Arc::new(StringArray::from(vec![label])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    fn heterogeneous_property_batch(uuid: u128, property: usize) -> RecordBatch {
        let mut fields = CONSTRUCTION_NODE_SCHEMA
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(format!("p{property:03}"), DataType::Int64, true));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(&[uuid.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["Person"])),
                Arc::new(Int64Array::from(vec![property as i64])),
            ],
        )
        .unwrap()
    }

    fn edge_property_batch(first: u128, rows: usize) -> RecordBatch {
        let edges = (first..first + rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let src = (1..=rows as u128)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let dst = (2..=rows as u128 + 1)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let mut fields = CONSTRUCTION_EDGE_SCHEMA
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("weight", DataType::Int64, true));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(fixed(&edges)),
                Arc::new(StringArray::from(vec!["R"; rows])),
                Arc::new(fixed(&src)),
                Arc::new(fixed(&dst)),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row as i64 + 20),
                )),
            ],
        )
        .unwrap()
    }

    fn semantic_authority(mode: graphforge_core::OntologyMode) -> ConstructionSemanticAuthority {
        let document = graphforge_ontology::OntologyDoc {
            ontology_id: "https://graphforge.dev/ontology/construction-test".into(),
            version: "1.0.0".into(),
            entity_types: vec![
                graphforge_ontology::EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                },
                graphforge_ontology::EntityTypeDef {
                    name: "Child".into(),
                    r#abstract: false,
                    parent: Some("Person".into()),
                },
            ],
            relation_types: vec![graphforge_ontology::RelationTypeDef {
                name: "R".into(),
                src: "Person".into(),
                dst: "Person".into(),
                inverse: None,
                semantic: graphforge_ontology::SemanticFlags::default(),
            }],
            properties: vec![
                graphforge_ontology::PropertyDef {
                    owner: "Person".into(),
                    name: "score".into(),
                    value_type: graphforge_ontology::PropertyValueType::Int64,
                    nullable: true,
                    multivalued: false,
                    default_json: None,
                },
                graphforge_ontology::PropertyDef {
                    owner: "Child".into(),
                    name: "nickname".into(),
                    value_type: graphforge_ontology::PropertyValueType::Utf8,
                    nullable: true,
                    multivalued: false,
                    default_json: None,
                },
                graphforge_ontology::PropertyDef {
                    owner: "R".into(),
                    name: "weight".into(),
                    value_type: graphforge_ontology::PropertyValueType::Int64,
                    nullable: true,
                    multivalued: false,
                    default_json: None,
                },
            ],
            constraints: vec![],
            migrations: vec![],
        };
        let value = serde_json::to_value(&document).unwrap();
        let legacy = crate::WorkspaceOntology {
            contract_version: 1,
            mode: match mode {
                graphforge_core::OntologyMode::Strict => crate::WorkspaceOntologyMode::Strict,
                graphforge_core::OntologyMode::Advisory => crate::WorkspaceOntologyMode::Advisory,
                graphforge_core::OntologyMode::Exploratory => {
                    crate::WorkspaceOntologyMode::Advisory
                }
            },
            source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
            canonical_ontology_sha256: Some(sha256(&serde_json::to_vec(&value).unwrap())),
            canonical_ontology: Some(value),
        };
        let composition = crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
            .unwrap()
            .unwrap();
        let bindings =
            crate::SemanticStorageBindings::project(&composition.compile().unwrap(), None).unwrap();
        ConstructionSemanticAuthority {
            composition,
            bindings,
        }
    }

    fn colliding_module_authority() -> ConstructionSemanticAuthority {
        let documents = ["alpha", "beta"].map(|name| graphforge_ontology::OntologyDoc {
            ontology_id: format!("https://graphforge.dev/ontology/{name}"),
            version: "1.0.0".into(),
            entity_types: vec![graphforge_ontology::EntityTypeDef {
                name: "Thing".into(),
                r#abstract: false,
                parent: None,
            }],
            relation_types: vec![],
            properties: vec![graphforge_ontology::PropertyDef {
                owner: "Thing".into(),
                name: "value".into(),
                value_type: graphforge_ontology::PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            }],
            constraints: vec![],
            migrations: vec![],
        });
        let modules = documents
            .into_iter()
            .map(|doc| graphforge_ontology::AuthoredModule {
                id: graphforge_ontology::OntologyModuleId {
                    ontology_id: doc.ontology_id.clone(),
                    authored_version: doc.version.clone(),
                    canonical_digest: graphforge_ontology::module_document_digest(&doc).unwrap(),
                },
                dependencies: vec![],
                doc,
                allow_projected_identity: false,
            })
            .collect::<Vec<_>>();
        let compiled =
            graphforge_ontology::compile_inventory(graphforge_ontology::InventoryCompileRequest {
                modules: &modules,
                bridges: &[],
                activation: &[],
                profile_default: graphforge_ontology::ActivationMode::Strict,
                limits: graphforge_ontology::CompositionLimits::default(),
                cancelled: None,
            })
            .unwrap();
        let composition = crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![]);
        let bindings = crate::SemanticStorageBindings::project(&compiled, None).unwrap();
        ConstructionSemanticAuthority {
            composition,
            bindings,
        }
    }

    fn open(root: &TempDir, operation: u128) -> GraphConstructionSession {
        GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(operation),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap()
    }

    fn nonempty_project() -> TempDir {
        nonempty_project_with_nodes(2)
    }

    fn nonempty_project_with_nodes(node_count: u64) -> TempDir {
        let project = TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join("topology")).unwrap();
        std::fs::write(
            project.path().join("topology/generation.json"),
            br#"{"topology_generation":1,"search_generation":1}"#,
        )
        .unwrap();
        let tails_schema = Arc::new(Schema::new(vec![
            Field::new("max_node_id", DataType::UInt64, false),
            Field::new("max_edge_id", DataType::UInt64, false),
        ]));
        let tails = RecordBatch::try_new(
            tails_schema.clone(),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![node_count])),
                Arc::new(arrow::array::UInt64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(project.path().join("topology/surrogate_tails.parquet")).unwrap(),
            tails_schema,
            None,
        )
        .unwrap();
        writer.write(&tails).unwrap();
        writer.close().unwrap();
        crate::uuid_membership::append_uuid_membership_delta(
            project.path(),
            1,
            &(1..=node_count)
                .map(|value| (Uuid::from_u128(u128::from(value)), value))
                .collect::<Vec<_>>(),
            &[Uuid::from_u128(100)],
        )
        .unwrap();
        project
    }

    fn nonempty_project_generation_two() -> TempDir {
        let project = nonempty_project_with_nodes(2);
        std::fs::write(
            project.path().join("topology/generation.json"),
            br#"{"topology_generation":2,"search_generation":2}"#,
        )
        .unwrap();
        crate::uuid_membership::append_uuid_membership_delta(
            project.path(),
            2,
            &[(Uuid::from_u128(3), 3)],
            &[Uuid::from_u128(101)],
        )
        .unwrap();
        let tails_schema = Arc::new(Schema::new(vec![
            Field::new("max_node_id", DataType::UInt64, false),
            Field::new("max_edge_id", DataType::UInt64, false),
        ]));
        let tails = RecordBatch::try_new(
            tails_schema.clone(),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![3])),
                Arc::new(arrow::array::UInt64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(project.path().join("topology/surrogate_tails.parquet")).unwrap(),
            tails_schema,
            None,
        )
        .unwrap();
        writer.write(&tails).unwrap();
        writer.close().unwrap();
        project
    }

    #[test]
    fn parent_identity_payload_is_referenced_not_copied_at_1x_and_2x_base() {
        for (base_nodes, operation) in [(2_u64, 8_110_u128), (4, 8_111)] {
            let project = nonempty_project_with_nodes(base_nodes);
            let mut session = GraphConstructionSession::open(
                project.path(),
                Uuid::from_u128(operation),
                1,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            session
                .append(
                    ConstructionChunkKind::Node,
                    "delta",
                    &node_batch(u128::from(base_nodes + 1), 1),
                )
                .unwrap();
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            let root = project
                .path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(operation).simple().to_string());
            assert_eq!(
                std::fs::metadata(root.join(shape.identities))
                    .unwrap()
                    .len(),
                32
            );
            assert_eq!(shape.node_count, base_nodes + 1);
        }
    }

    #[test]
    fn row_artifact_retains_dynamic_properties_and_is_uuid_sorted() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 99);
        let schema = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(fixed(&[3_u128.to_be_bytes(), 1_u128.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["Person", "Person"])),
                Arc::new(Int64Array::from(vec![30, 10])),
            ],
        )
        .unwrap();
        let receipt = session
            .append(ConstructionChunkKind::Node, "properties", &batch)
            .unwrap();
        let file = session
            .root
            .open_child_file(OsStr::new(&receipt.parquet.name))
            .unwrap();
        let batches = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let uuids = uuid_column(&batches[0], "node_uuid").unwrap();
        let scores = batches[0]
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(uuid_value(uuids, 0).unwrap(), 1_u128.to_be_bytes());
        assert_eq!(scores.values(), &[10, 30]);
        drop(session);

        let resumed = open(&root, 99);
        assert!(resumed.checkpoint.session_now_micros > 0);
        assert_eq!(
            resumed.read_receipt(0).unwrap().schema_sha256,
            receipt.schema_sha256
        );
    }

    #[test]
    fn replay_binds_each_property_schema_and_rejects_unsupported_staging_types() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 98);
        let batch_with = |property: &str| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                    Field::new("label", DataType::Utf8, false),
                    Field::new(property, DataType::Int64, false),
                ])),
                vec![
                    Arc::new(fixed(&[1_u128.to_be_bytes()])),
                    Arc::new(StringArray::from(vec!["Person"])),
                    Arc::new(Int64Array::from(vec![7])),
                ],
            )
            .unwrap()
        };
        session
            .append(
                ConstructionChunkKind::Node,
                "schema-bound",
                &batch_with("score"),
            )
            .unwrap();
        assert!(
            session
                .append(
                    ConstructionChunkKind::Node,
                    "schema-bound",
                    &batch_with("renamed")
                )
                .unwrap_err()
                .to_string()
                .contains("conflicting")
        );
        let heterogeneous = session
            .append(
                ConstructionChunkKind::Node,
                "different-chunk-schema",
                &batch_with("renamed"),
            )
            .unwrap();
        assert_ne!(
            heterogeneous.schema_sha256,
            session.read_receipt(0).unwrap().schema_sha256
        );

        let unsupported = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("payload", DataType::Binary, false),
            ])),
            vec![
                Arc::new(fixed(&[2_u128.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["Person"])),
                Arc::new(BinaryArray::from(vec![b"bytes".as_slice()])),
            ],
        )
        .unwrap();
        assert!(
            session
                .append(ConstructionChunkKind::Node, "unsupported", &unsupported)
                .unwrap_err()
                .to_string()
                .contains("unsupported")
        );
    }

    #[test]
    fn catalog_shape_preserves_parent_ids_history_and_ignores_null_observations() {
        let project = TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join("topology")).unwrap();
        let mut parent = RuntimeCatalog::new();
        parent.intern_label_at("BaseOnly", 11);
        parent.intern_property_at("legacy", Some("BaseOnly"), 11);
        let parent_path = project.path().join("topology/runtime_catalog.parquet");
        let parent_batch = parent.to_record_batch();
        let mut parent_writer = ArrowWriter::try_new(
            File::create(&parent_path).unwrap(),
            parent_batch.schema(),
            None,
        )
        .unwrap();
        parent_writer.write(&parent_batch).unwrap();
        parent_writer.close().unwrap();

        let private_path = project.path().join("catalog-shape");
        std::fs::create_dir(&private_path).unwrap();
        let private = StableDirectory::open(&private_path).unwrap();
        let rows = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("score", DataType::Int64, true),
            ])),
            vec![
                Arc::new(fixed(&[1_u128.to_be_bytes(), 2_u128.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["Person", "Person"])),
                Arc::new(Int64Array::from(vec![Some(7), None])),
            ],
        )
        .unwrap();
        write_parquet(&private, "nodes.parquet", &rows).unwrap();
        let mut evidence = GraphConstructionEvidence::default();
        let project_root = StableDirectory::open(project.path()).unwrap();
        let (parent_catalog, parent_catalog_sha256, _) =
            load_parent_runtime_catalog(&project_root, 1, GraphConstructionBudgets::default())
                .unwrap();
        assert!(parent_catalog_sha256.is_some());
        let output = build_runtime_catalog(
            parent_catalog,
            &private,
            &["nodes.parquet".to_owned()],
            &[],
            42,
            GraphConstructionBudgets::default(),
            &mut || false,
            &mut evidence,
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            private.open_child_file(OsStr::new(&output)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let kinds = merged
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let names = merged
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = merged
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let first = merged
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        let base = (0..merged.num_rows())
            .find(|&row| kinds.value(row) == "entity_type" && names.value(row) == "BaseOnly")
            .unwrap();
        assert_eq!((counts.value(base), first.value(base)), (1, 11));
        let score = (0..merged.num_rows())
            .find(|&row| kinds.value(row) == "property" && names.value(row) == "score")
            .unwrap();
        assert_eq!(counts.value(score), 1);
    }

    #[test]
    fn journal_is_constant_control_state_and_seal_reopens_every_artifact() {
        for chunks in [1_u64, 2, 4] {
            let root = TempDir::new().unwrap();
            let operation = 100 + u128::from(chunks);
            let mut session = open(&root, operation);
            for chunk in 0..chunks {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("n-{chunk}"),
                        &node_batch(1 + u128::from(chunk) * 32, 32),
                    )
                    .unwrap();
            }
            assert_eq!(session.accepted_chunks(), chunks);
            assert_eq!(session.evidence().input_rows, chunks * 32);
            assert_eq!(session.evidence().peak_batch_rows, 32);
            assert_eq!(session.evidence().peak_run_records, 64);
            assert_eq!(session.evidence().prior_topology_rows_decoded, 0);
            assert_eq!(session.evidence().current_transitions, 0);
            assert!(session.evidence().write_operations < session.evidence().input_rows);
            assert!(session.evidence().fsync_operations > 0);
            assert!(session.evidence().peak_accounted_live_bytes > 0);
            let checkpoint_bytes = session
                .root
                .open_child_file(OsStr::new(CHECKPOINT))
                .unwrap()
                .metadata()
                .unwrap()
                .len();
            assert!(checkpoint_bytes < MAX_CONTROL_BYTES);
            session.seal().unwrap();
            assert_eq!(session.state(), GraphConstructionState::Sealed);
            assert!(session.evidence().authentication_read_bytes > 0);
        }
    }

    #[test]
    fn million_chunk_online_scheduler_has_logarithmic_name_state() {
        let slots = online_merge_name_slot_bound(1_000_000, 32);
        assert!(slots <= 31 * 4, "retained slots: {slots}");
        assert!(slots < 1_000_000 / 1_000);
        assert_eq!(GraphConstructionBudgets::default().max_schema_groups, 256);
    }

    #[test]
    fn resolved_endpoint_windows_use_logarithmic_name_state_at_1x_2x_4x() {
        for windows in [1_u64, 2, 4] {
            let root = TempDir::new().unwrap();
            let budgets = GraphConstructionBudgets {
                max_batch_rows: 2,
                max_run_records: 8,
                merge_fan_in: 2,
                ..GraphConstructionBudgets::default()
            };
            let mut session = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(7_100 + u128::from(windows)),
                0,
                budgets,
            )
            .unwrap();
            let nodes = windows + 1;
            for offset in (0..nodes).step_by(2) {
                let rows = usize::try_from((nodes - offset).min(2)).unwrap();
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{offset}"),
                        &node_batch(1 + u128::from(offset), rows),
                    )
                    .unwrap();
            }
            for offset in (0..windows).step_by(2) {
                let rows = usize::try_from((windows - offset).min(2)).unwrap();
                session
                    .append(
                        ConstructionChunkKind::Edge,
                        &format!("edges-{offset}"),
                        &edge_batch(10_000 + u128::from(offset), rows),
                    )
                    .unwrap();
            }
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            let expected_slots = u64::from(windows.ilog2()).max(1);
            assert!(
                session.evidence().peak_resolved_endpoint_name_slots <= expected_slots,
                "windows={windows} slots={}",
                session.evidence().peak_resolved_endpoint_name_slots
            );
            let shaped_outputs = std::iter::once(&shape.identities)
                .chain(shape.node_details.iter())
                .chain(shape.edge_details.iter())
                .chain(shape.node_rows.iter())
                .chain(shape.edge_rows.iter())
                .chain(shape.edge_endpoints.iter())
                .chain(std::iter::once(&shape.runtime_catalog));
            let expected_payload_bytes = shaped_outputs
                .clone()
                .map(|name| {
                    session
                        .root
                        .open_child_file(OsStr::new(name))
                        .unwrap()
                        .metadata()
                        .unwrap()
                        .len()
                })
                .sum::<u64>();
            let expected_capability_bytes = shaped_outputs
                .map(|name| {
                    session
                        .root
                        .open_child_file(OsStr::new(&shape_receipt_name(name)))
                        .unwrap()
                        .metadata()
                        .unwrap()
                        .len()
                })
                .sum::<u64>();
            assert_eq!(
                session.evidence().shaped_output_authentication_bytes,
                expected_capability_bytes
            );
            assert!(expected_capability_bytes < expected_payload_bytes);
            assert!(session.evidence().shaped_output_authentication_operations > 0);
        }
    }

    #[test]
    fn parent_catalog_streaming_enforces_entry_and_decoded_byte_budgets() {
        let root = TempDir::new().unwrap();
        let project = StableDirectory::open(root.path()).unwrap();
        let topology = project
            .create_child_directory(OsStr::new("topology"))
            .unwrap();
        let mut source = RuntimeCatalog::new();
        for index in 0..5_000 {
            source.intern_label_at(&format!("Label{index:05}"), 42);
        }
        write_parquet(
            &topology,
            "runtime_catalog.parquet",
            &source.to_record_batch(),
        )
        .unwrap();

        let too_few = GraphConstructionBudgets {
            max_catalog_entries: 4_999,
            ..GraphConstructionBudgets::default()
        };
        assert!(
            load_parent_runtime_catalog(&project, 1, too_few)
                .unwrap_err()
                .to_string()
                .contains("admission budget")
        );
        let too_small = GraphConstructionBudgets {
            max_catalog_decoded_bytes: 1,
            ..GraphConstructionBudgets::default()
        };
        assert!(
            load_parent_runtime_catalog(&project, 1, too_small)
                .unwrap_err()
                .to_string()
                .contains("admission budget")
        );
        let (restored, digest, work) =
            load_parent_runtime_catalog(&project, 1, GraphConstructionBudgets::default()).unwrap();
        assert_eq!(restored.to_record_batch().num_rows(), 5_000);
        assert!(digest.is_some());
        assert!(work.bytes > 0);
        assert!(work.operations > 0);
    }

    #[test]
    fn schema_group_admission_is_constant_and_budgeted() {
        let root = TempDir::new().unwrap();
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(6_999),
            0,
            GraphConstructionBudgets {
                max_schema_groups: 1,
                ..GraphConstructionBudgets::default()
            },
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        let error = session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 1))
            .unwrap_err();
        assert!(error.to_string().contains("schema-group budget"));
        assert_eq!(session.checkpoint.node_schema_sha256.len(), 1);
        assert!(session.checkpoint.edge_schema_sha256.is_empty());
    }

    #[test]
    fn shape_control_encoding_fails_closed_above_its_separate_cap() {
        let oversized = "x".repeat((MAX_SHAPE_CONTROL_BYTES + 1) as usize);
        let error = encode_control(&oversized, SHAPE_INTENT).unwrap_err();
        assert!(error.to_string().contains("control record exceeds bound"));
        assert_eq!(control_limit(CHECKPOINT), MAX_CONTROL_BYTES);
    }

    #[test]
    fn shaping_is_bounded_deterministic_and_multipass_at_1x_2x_4x() {
        for chunks in [1_usize, 2, 4] {
            let root = TempDir::new().unwrap();
            let mut session = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(7_000 + chunks as u128),
                0,
                GraphConstructionBudgets {
                    merge_fan_in: 2,
                    ..GraphConstructionBudgets::default()
                },
            )
            .unwrap();
            for chunk in 0..chunks {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{chunk}"),
                        &node_batch(1 + chunk as u128 * 4, 4),
                    )
                    .unwrap();
            }
            session
                .append(
                    ConstructionChunkKind::Edge,
                    "edges",
                    &edge_batch(10_000, chunks * 2),
                )
                .unwrap();
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(shape.node_count, (chunks * 4) as u64);
            assert_eq!(shape.edge_count, (chunks * 2) as u64);
            assert_eq!(shape.max_node_surrogate, (chunks * 4) as u64);
            assert_eq!(shape.max_edge_surrogate, (chunks * 2) as u64);
            assert_eq!(shape.node_rows.len(), 1);
            assert_eq!(shape.edge_rows.len(), 1);
            assert!(shape.edge_endpoints.is_some());
            assert!(
                session
                    .root
                    .open_child_file(OsStr::new(&shape.runtime_catalog))
                    .is_ok()
            );
            assert!(session.evidence().peak_merge_inputs <= 2);
            assert!(session.evidence().merge_read_bytes > 0);
            assert!(session.evidence().merge_written_bytes > 0);
            assert!(session.evidence().merge_read_blocks > 0);
            assert!(session.evidence().merge_write_blocks > 0);
            assert!(session.evidence().merge_fsync_operations > 0);
            assert!(session.evidence().parquet_read_operations > 0);
            assert!(session.evidence().parquet_write_operations > 0);
            if chunks == 4 {
                assert!(session.evidence().merge_passes >= 2);
            }
        }
    }

    #[test]
    fn batch_partition_and_resume_produce_identical_canonical_data_fingerprints() {
        let one = TempDir::new().unwrap();
        let mut one_session = open(&one, 7_100);
        one_session
            .append(ConstructionChunkKind::Node, "all", &node_batch(1, 8))
            .unwrap();
        one_session.seal().unwrap();
        let one_shape = one_session
            .shape_canonical_with_cancellation(|| false)
            .unwrap();
        let one_identity = receipt_for_existing(&one_session.root, &one_shape.identities).unwrap();
        let one_rows = receipt_for_existing(&one_session.root, &one_shape.node_rows[0]).unwrap();

        let two = TempDir::new().unwrap();
        let mut two_session = open(&two, 7_101);
        two_session
            .append(ConstructionChunkKind::Node, "first", &node_batch(1, 4))
            .unwrap();
        two_session
            .append(ConstructionChunkKind::Node, "second", &node_batch(5, 4))
            .unwrap();
        two_session.seal().unwrap();
        drop(two_session);
        let mut resumed = open(&two, 7_101);
        let two_shape = resumed.shape_canonical_with_cancellation(|| false).unwrap();
        let two_identity = receipt_for_existing(&resumed.root, &two_shape.identities).unwrap();
        let two_rows = receipt_for_existing(&resumed.root, &two_shape.node_rows[0]).unwrap();
        assert_eq!(one_identity.sha256, two_identity.sha256);
        assert_eq!(one_rows.sha256, two_rows.sha256);
        assert_eq!((one_shape.node_count, one_shape.edge_count), (8, 0));
        assert_eq!((two_shape.node_count, two_shape.edge_count), (8, 0));
    }

    #[test]
    fn immediate_seal_prepare_authenticates_staged_bytes_once_with_constant_factor() {
        let mut observations = Vec::new();
        for rows in [64_usize, 128, 256] {
            let root = TempDir::new().unwrap();
            crate::open_or_initialize_project(root.path()).unwrap();
            let mut session = GraphConstructionSession::open(
                root.path(),
                Uuid::now_v7(),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, rows))
                .unwrap();
            session
                .seal_and_prepare_canonical_encoding_with_cancellation(1, || false)
                .unwrap();
            let evidence = session.evidence();
            assert_eq!(evidence.authentication_read_bytes, 0);
            assert!(evidence.shape_input_validation_read_bytes > 0);
            assert!(
                evidence.shaped_output_authentication_bytes < evidence.canonical_output_bytes / 4,
                "writer-completed shaped outputs were reopened: auth={} canonical={}",
                evidence.shaped_output_authentication_bytes,
                evidence.canonical_output_bytes
            );
            assert!(
                evidence.shape_input_validation_read_bytes
                    <= evidence.write_bytes.saturating_mul(3),
                "shape authentication exceeded three staged-byte passes"
            );
            observations.push((rows, evidence.shape_input_validation_read_bytes));
        }
        for pair in observations.windows(2) {
            let (smaller_rows, smaller_bytes) = pair[0];
            let (larger_rows, larger_bytes) = pair[1];
            assert_eq!(larger_rows, smaller_rows * 2);
            assert!(larger_bytes >= smaller_bytes);
            assert!(
                larger_bytes <= smaller_bytes.saturating_mul(3),
                "doubling rows exceeded the bounded linear byte envelope"
            );
        }
    }

    #[test]
    fn interrupted_immediate_seal_reauthenticates_before_resume_consumption() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let operation = Uuid::now_v7();
        let mut session = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 64))
            .unwrap();
        assert!(
            session
                .seal_and_prepare_canonical_encoding_with_cancellation(1, || true)
                .is_err()
        );
        assert_eq!(session.state(), GraphConstructionState::Sealed);
        assert_eq!(session.evidence().authentication_read_bytes, 0);
        drop(session);

        let mut resumed = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        resumed
            .prepare_canonical_encoding_with_cancellation(1, || false)
            .unwrap();
        assert!(resumed.evidence().shape_input_validation_read_bytes > 0);
    }

    #[test]
    fn shaping_rejects_cross_kind_duplicates_and_missing_endpoints() {
        let root = TempDir::new().unwrap();
        let mut duplicate = open(&root, 8_001);
        duplicate
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        duplicate
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(1, 1))
            .unwrap();
        duplicate.seal().unwrap();
        assert!(
            duplicate
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("duplicate identity")
        );

        let root = TempDir::new().unwrap();
        let mut missing = open(&root, 8_002);
        missing
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        missing
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(20_000, 2))
            .unwrap();
        missing.seal().unwrap();
        assert!(
            missing
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("endpoint")
        );
    }

    #[test]
    fn nonempty_base_rejects_duplicate_cross_kind_and_missing_endpoint_without_copy() {
        let project = nonempty_project();
        let budgets = GraphConstructionBudgets::default();

        let mut duplicate =
            GraphConstructionSession::open(project.path(), Uuid::from_u128(8_101), 1, budgets)
                .unwrap();
        duplicate
            .append(ConstructionChunkKind::Node, "duplicate", &node_batch(1, 1))
            .unwrap();
        duplicate.seal().unwrap();
        assert!(
            duplicate
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("conflicts with pinned base")
        );
        assert!(
            !project
                .path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(8_101).simple().to_string())
                .join("base-identities.run")
                .exists()
        );
        drop(duplicate);

        let edge = |edge_uuid: u128, source: u128, target: u128| {
            RecordBatch::try_new(
                CONSTRUCTION_EDGE_SCHEMA.clone(),
                vec![
                    Arc::new(fixed(&[edge_uuid.to_be_bytes()])),
                    Arc::new(StringArray::from(vec!["R"])),
                    Arc::new(fixed(&[source.to_be_bytes()])),
                    Arc::new(fixed(&[target.to_be_bytes()])),
                ],
            )
            .unwrap()
        };
        let mut cross_kind =
            GraphConstructionSession::open(project.path(), Uuid::from_u128(8_102), 1, budgets)
                .unwrap();
        cross_kind
            .append(ConstructionChunkKind::Edge, "cross-kind", &edge(1, 1, 2))
            .unwrap();
        cross_kind.seal().unwrap();
        assert!(
            cross_kind
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("conflicts with pinned base")
        );
        drop(cross_kind);

        let mut missing =
            GraphConstructionSession::open(project.path(), Uuid::from_u128(8_103), 1, budgets)
                .unwrap();
        missing
            .append(ConstructionChunkKind::Edge, "missing", &edge(200, 999, 1))
            .unwrap();
        missing.seal().unwrap();
        assert!(
            missing
                .shape_canonical_with_cancellation(|| false)
                .unwrap_err()
                .to_string()
                .contains("endpoint")
        );

        drop(missing);
        let operation = Uuid::from_u128(8_104);
        let mut delta =
            GraphConstructionSession::open(project.path(), operation, 1, budgets).unwrap();
        delta
            .append(ConstructionChunkKind::Node, "one-new", &node_batch(3, 1))
            .unwrap();
        delta.seal().unwrap();
        let shape = delta.shape_canonical_with_cancellation(|| false).unwrap();
        let operation_root = project
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string());
        assert_eq!(
            std::fs::metadata(operation_root.join(&shape.identities))
                .unwrap()
                .len(),
            32
        );
        assert!(
            !operation_root
                .join("merge-identities-with-base.run")
                .exists()
        );
        assert_eq!(shape.parent_topology_generation, 1);
        assert!(shape.parent_uuid_manifest_sha256.is_some());

        drop(delta);
        let mut retained_endpoints =
            GraphConstructionSession::open(project.path(), Uuid::from_u128(8_105), 1, budgets)
                .unwrap();
        retained_endpoints
            .append(
                ConstructionChunkKind::Edge,
                "base-endpoints",
                &edge(200, 1, 2),
            )
            .unwrap();
        retained_endpoints.seal().unwrap();
        let shape = retained_endpoints
            .shape_canonical_with_cancellation(|| false)
            .unwrap();
        assert_eq!((shape.node_count, shape.edge_count), (2, 2));
        assert!(retained_endpoints.evidence().retained_probe_read_bytes > 0);
        assert!(retained_endpoints.evidence().retained_probe_block_loads > 0);
        assert!(
            retained_endpoints.evidence().retained_probe_read_bytes
                <= retained_endpoints
                    .evidence()
                    .retained_probe_block_loads
                    .saturating_mul(BLOCK_BYTES as u64)
        );
        let mut resolved = BufReader::new(
            retained_endpoints
                .root
                .open_child_file(OsStr::new(shape.edge_endpoints.as_ref().unwrap()))
                .unwrap(),
        );
        assert_eq!(
            u64::from_be_bytes(
                read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
                    .unwrap()
                    .unwrap()[24..32]
                    .try_into()
                    .unwrap()
            ),
            1
        );
        assert_eq!(
            u64::from_be_bytes(
                read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
                    .unwrap()
                    .unwrap()[24..32]
                    .try_into()
                    .unwrap()
            ),
            2
        );
    }

    #[test]
    fn shaping_cancellation_is_recovered_on_reopen() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 8_003);
        for chunk in 0..4 {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{chunk}"),
                    &node_batch(1 + chunk * 8, 8),
                )
                .unwrap();
        }
        session.seal().unwrap();
        let mut polls = 0;
        assert!(
            session
                .shape_canonical_with_cancellation(|| {
                    polls += 1;
                    polls > 6
                })
                .is_err()
        );
        drop(session);
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(8_003),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        resumed.shape_canonical_with_cancellation(|| false).unwrap();
    }

    #[test]
    fn fixed_merge_reader_rejects_truncation_and_self_loop_is_valid() {
        assert!(read_fixed::<16>(&mut std::io::Cursor::new(vec![0_u8; 15])).is_err());
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 8_004);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
            .unwrap();
        let endpoint = [1_u128.to_be_bytes()];
        let edge = RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&[9_000_u128.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["R"])),
                Arc::new(fixed(&endpoint)),
                Arc::new(fixed(&endpoint)),
            ],
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Edge, "self-loop", &edge)
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.edge_count, 1);
        let mut resolved = BufReader::new(
            session
                .root
                .open_child_file(OsStr::new(shape.edge_endpoints.as_ref().unwrap()))
                .unwrap(),
        );
        let source = read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
            .unwrap()
            .unwrap();
        let target = read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
            .unwrap()
            .unwrap();
        assert_eq!(&source[..16], &9_000_u128.to_be_bytes());
        assert_eq!((source[16], target[16]), (0, 1));
        assert_eq!(&source[24..32], &target[24..32]);
        assert!(
            read_fixed::<RESOLVED_ENDPOINT_WIDTH>(&mut resolved)
                .unwrap()
                .is_none()
        );
        drop(session);
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(8_004),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(
            resumed.shape_canonical_with_cancellation(|| false).unwrap(),
            shape
        );
    }

    #[test]
    fn logical_digest_is_independent_of_arrow_slice_layout() {
        let whole = node_batch(10, 6);
        let sliced = whole.slice(2, 3);
        let rebuilt = node_batch(12, 3);
        assert_eq!(
            logical_batch_digest(ConstructionChunkKind::Node, &sliced).unwrap(),
            logical_batch_digest(ConstructionChunkKind::Node, &rebuilt).unwrap()
        );
    }

    #[test]
    fn replay_is_idempotent_and_reauthenticates_artifacts() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 200);
        let batch = node_batch(1, 8);
        let first = session
            .append(ConstructionChunkKind::Node, "nodes", &batch)
            .unwrap();
        assert_eq!(
            session
                .append(ConstructionChunkKind::Node, "nodes", &batch)
                .unwrap(),
            first
        );
        assert_eq!(session.accepted_chunks(), 1);
        assert_eq!(session.evidence().replayed_chunks, 1);
        assert!(session.evidence().replay_validation_read_bytes > 0);
        assert!(session.evidence().replay_validation_read_operations > 0);
        assert!(
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 9))
                .is_err()
        );
    }

    #[test]
    fn staged_catalog_cardinality_is_bounded_at_one_and_two_windows() {
        for windows in [1_usize, 2] {
            let root = TempDir::new().unwrap();
            let rows = windows * 8;
            let expected_identifier_bytes = rows * "Label00000000".len();
            let budgets = GraphConstructionBudgets {
                max_batch_rows: 8,
                max_run_records: 32,
                max_catalog_entries: rows,
                max_catalog_identifier_bytes: expected_identifier_bytes,
                ..GraphConstructionBudgets::default()
            };
            let mut session = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(8_000 + rows as u128),
                0,
                budgets,
            )
            .unwrap();
            for window in 0..windows {
                session
                    .append(
                        ConstructionChunkKind::Node,
                        &format!("nodes-{window}"),
                        &distinct_label_batch(1 + (window * 8) as u128, 8),
                    )
                    .unwrap();
            }
            session.seal().unwrap();
            session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(session.evidence().peak_catalog_entries, rows as u64);
            assert_eq!(
                session.evidence().peak_catalog_identifier_bytes,
                expected_identifier_bytes as u64
            );
            assert!(session.evidence().peak_catalog_decoded_batch_bytes > 0);
            assert!(session.evidence().shape_input_validation_read_bytes > 0);
        }
    }

    #[test]
    fn staged_catalog_rejects_entry_and_identifier_overflow_before_interning() {
        for budgets in [
            GraphConstructionBudgets {
                max_batch_rows: 8,
                max_run_records: 32,
                max_catalog_entries: 7,
                ..GraphConstructionBudgets::default()
            },
            GraphConstructionBudgets {
                max_batch_rows: 8,
                max_run_records: 32,
                max_catalog_identifier_bytes: 7 * "Label00000000".len(),
                ..GraphConstructionBudgets::default()
            },
        ] {
            let root = TempDir::new().unwrap();
            let mut session =
                GraphConstructionSession::open(root.path(), Uuid::new_v4(), 0, budgets).unwrap();
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &distinct_label_batch(1, 8),
                )
                .unwrap();
            session.seal().unwrap();
            assert!(
                session
                    .shape_canonical_with_cancellation(|| false)
                    .unwrap_err()
                    .to_string()
                    .contains("catalog admission budget")
            );
        }
    }

    #[test]
    fn cancellation_recovers_private_intent_without_accepting_a_chunk() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(300);
        let mut session = open(&root, 300);
        let mut polls = 0_u8;
        assert!(
            session
                .append_with_cancellation(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_batch(1, 8),
                    || {
                        polls = polls.saturating_add(1);
                        polls == 2
                    },
                )
                .is_err()
        );
        drop(session);
        let resumed = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(resumed.accepted_chunks(), 0);
        assert!(resumed.root.open_child_file(OsStr::new(INTENT)).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn session_drop_unlocks_before_a_duplicated_descriptor_closes() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(301);
        let session = open(&root, 301);
        let inherited_descriptor = session.session_lock.try_clone().unwrap();

        drop(session);

        let resumed = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(resumed.accepted_chunks(), 0);
        drop(resumed);
        drop(inherited_descriptor);
    }

    #[test]
    fn node_after_edge_and_concurrent_same_process_open_fail_closed() {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 400);
        assert!(
            GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(400),
                0,
                GraphConstructionBudgets::default()
            )
            .is_err()
        );
        session
            .append(ConstructionChunkKind::Edge, "edges", &edge_batch(100, 2))
            .unwrap();
        assert!(
            session
                .append(ConstructionChunkKind::Node, "late-node", &node_batch(1, 1))
                .is_err()
        );
    }

    #[test]
    fn session_coordination_file_is_exclusively_locked() {
        let root = TempDir::new().unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        let owner = stable
            .open_or_create_child_file(OsStr::new(SESSION_LOCK))
            .unwrap();
        assert!(crate::file_lock::try_lock_exclusive(&owner).unwrap());

        let contender = stable.open_child_file(OsStr::new(SESSION_LOCK)).unwrap();
        assert!(!crate::file_lock::try_lock_exclusive(&contender).unwrap());
        crate::file_lock::unlock(&owner).unwrap();
        assert!(crate::file_lock::try_lock_exclusive(&contender).unwrap());
    }

    #[test]
    fn counting_reader_uses_authenticated_cas_length() {
        let root = TempDir::new().unwrap();
        let payload = b"authenticated parquet-sized payload";
        let (digest, _) = crate::install_graph_object_bytes(root.path(), payload).unwrap();
        let file = crate::graph_object_store::open_graph_object_by_digest(
            root.path(),
            &digest,
            payload.len() as u64,
        )
        .unwrap();
        let reader = CountingChunkReader {
            file,
            counter: IoCounter::default(),
        };
        assert_eq!(Length::len(&reader), payload.len() as u64);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_substitution_is_rejected_on_independent_seal() {
        use std::os::unix::fs::symlink;

        let root = TempDir::new().unwrap();
        let mut session = open(&root, 500);
        let receipt = session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        let operation_root = root
            .path()
            .join(PRIVATE_ROOT)
            .join(Uuid::from_u128(500).simple().to_string());
        let artifact = operation_root.join(&receipt.identities.name);
        let displaced = operation_root.join("displaced.run");
        std::fs::rename(&artifact, &displaced).unwrap();
        symlink(&displaced, &artifact).unwrap();
        assert!(session.seal().is_err());
    }

    #[test]
    fn crash_subprocess_helper() {
        let Ok(root) = std::env::var("GF_CONSTRUCTION_CRASH_ROOT") else {
            return;
        };
        if std::env::var_os("GF_CONSTRUCTION_PUBLICATION_CRASH").is_some() {
            crate::open_or_initialize_project(Path::new(&root)).unwrap();
            let mut session = GraphConstructionSession::open(
                Path::new(&root),
                Uuid::from_u128(9_470),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            session
                .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
                .unwrap();
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            let encoding = session.encode_canonical(&shape, 1).unwrap();
            session
                .publish_canonical(&encoding, Uuid::from_u128(9_471), Uuid::from_u128(9_472))
                .unwrap();
            return;
        }
        let mut session = GraphConstructionSession::open(
            Path::new(&root),
            Uuid::from_u128(600),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 8))
            .unwrap();
        if std::env::var_os("GF_CONSTRUCTION_UUID_ENCODE_CRASH").is_some() {
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            session.encode_canonical(&shape, 1).unwrap();
        }
        if std::env::var_os("GF_CONSTRUCTION_SHAPE_CRASH").is_some() {
            session
                .append(ConstructionChunkKind::Node, "nodes-2", &node_batch(9, 8))
                .unwrap();
            session.seal().unwrap();
            session.shape_canonical_with_cancellation(|| false).unwrap();
        }
    }

    #[test]
    fn publication_crash_after_current_finalizes_same_target_on_reopen() {
        let root = TempDir::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("graph_construction::tests::crash_subprocess_helper")
            .arg("--nocapture")
            .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
            .env("GF_CONSTRUCTION_PUBLICATION_CRASH", "1")
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env(
                "GF_CONSTRUCTION_FAILPOINT",
                "publication.after_current_before_receipt",
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));

        let target = Uuid::from_u128(9_471);
        assert_eq!(
            crate::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            target
        );
        let operation = Uuid::from_u128(9_470);
        let operation_root = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string());
        assert!(!operation_root.join(PUBLICATION_RECEIPT).exists());
        let encoding: GraphConstructionEncoding = serde_json::from_slice(
            &std::fs::read(operation_root.join("encoded-v1/inventory.json")).unwrap(),
        )
        .unwrap();
        let mut resumed = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        let receipt = resumed
            .publish_canonical(&encoding, target, Uuid::from_u128(9_472))
            .unwrap();
        assert_eq!(receipt.generation_uuid, target);
        assert!(receipt.idempotent_replay);
        assert!(operation_root.join(PUBLICATION_RECEIPT).is_file());

        let current = crate::resolve_project_generation(root.path()).unwrap();
        assert_eq!(current.generation_uuid(), target);
        let inventory = current.graph_files_inventory().unwrap().unwrap();
        let materialized = TempDir::new().unwrap();
        let graph = materialized.path().join("graph");
        std::fs::create_dir(&graph).unwrap();
        crate::materialize_graph_objects(root.path(), &inventory, &graph).unwrap();
        let uuid_index = crate::UuidMembershipIndex::open(&graph).unwrap();
        assert_eq!(uuid_index.count(crate::UuidIndexKind::Node), 2);
    }

    #[test]
    fn uuid_encoding_crashes_recover_every_durable_boundary() {
        for failpoint in [
            "encode.parquet.after_temp_fsync.topology/nodes/00000000000000000001-00000000000000000008.parquet",
            "encode.parquet.after_install.topology/nodes/00000000000000000001-00000000000000000008.parquet",
            "encode.copy.after_temp_fsync.topology/runtime_catalog.parquet",
            "encode.copy.after_install.topology/runtime_catalog.parquet",
            "uuid_encode.after_intent",
            "uuid_encode.after_temps",
            "uuid_encode.after_delta_runs",
            "uuid_encode.after_manifest",
            "uuid_encode.after_intent_removal",
            "v4_publish.after_artifacts",
            "v4_publish.after_artifacts_fsync",
            "v4_publish.after_receipt_temp_fsync",
            "v4_publish.after_receipt_install",
            "v4_publish.after_manifest_temp_fsync",
            "v4_publish.after_manifest_install",
            "v4_publish.after_lock_temp_fsync",
            "v4_publish.after_lock_install",
            "encode.after_v4_before_inventory",
            "encode.control.after_temp_fsync.inventory.json",
            "encode.control.after_install.inventory.json",
        ] {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::crash_subprocess_helper")
                .arg("--nocapture")
                .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
                .env("GF_CONSTRUCTION_UUID_ENCODE_CRASH", "1")
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            let mut resumed = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(600),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            let shape = resumed.shape_canonical_with_cancellation(|| false).unwrap();
            let encoded = resumed.encode_canonical(&shape, 1).unwrap();
            assert_eq!(encoded.evidence.membership_records, 8, "{failpoint}");
            let membership = root
                .path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(600).simple().to_string())
                .join("encoded-v1/graph/topology/uuid-membership");
            assert!(!membership.join(".construction-intent.json").exists());
            assert!(std::fs::read_dir(membership).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp")
            }));
            let encoded_root = root
                .path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(600).simple().to_string())
                .join("encoded-v1");
            assert!(tree_has_no_temps(&encoded_root));
            assert!(!encoded_root.join("encoding-intent.json").exists());
        }
    }

    #[test]
    fn subprocess_crashes_recover_each_durable_boundary() {
        let receipt = receipt_name(0);
        let key = chunk_key_name("nodes");
        let parquet = format!("{}.parquet", artifact_stem(0, ConstructionChunkKind::Node));
        let cases = vec![
            (
                "control.install.after_partial.checkpoint.json".to_owned(),
                0_u64,
            ),
            (
                "control.install.after_temp_fsync.checkpoint.json".to_owned(),
                0_u64,
            ),
            (
                "control.install.after_install.checkpoint.json".to_owned(),
                0,
            ),
            ("control.install.after_temp_fsync.intent.json".to_owned(), 0),
            ("control.install.after_install.intent.json".to_owned(), 0),
            (format!("artifact.after_temp_fsync.{parquet}"), 0),
            (format!("artifact.after_install.{parquet}"), 0),
            ("control.replace.after_replace.intent.json".to_owned(), 0),
            (format!("control.install.after_partial.{receipt}"), 0),
            (format!("control.install.after_temp_fsync.{receipt}"), 0),
            (format!("control.install.after_install.{receipt}"), 1),
            (format!("control.install.after_temp_fsync.{key}"), 1),
            (format!("control.install.after_install.{key}"), 1),
            (
                "control.replace.after_partial.checkpoint.json".to_owned(),
                1,
            ),
            (
                "control.replace.after_temp_fsync.checkpoint.json".to_owned(),
                1,
            ),
            (
                "control.replace.after_replace.checkpoint.json".to_owned(),
                1,
            ),
        ];
        for (failpoint, accepted) in cases {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::crash_subprocess_helper")
                .arg("--nocapture")
                .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", &failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "failpoint {failpoint}");
            let mut resumed = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(600),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            assert_eq!(resumed.accepted_chunks(), accepted, "{failpoint}");
            if accepted == 0 {
                resumed
                    .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 8))
                    .unwrap();
            }
            resumed.seal().unwrap();
        }
    }

    #[test]
    fn shape_inventory_and_evidence_commit_recover_without_double_counting() {
        let reference_root = TempDir::new().unwrap();
        let mut reference = GraphConstructionSession::open(
            reference_root.path(),
            Uuid::from_u128(600),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        reference
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 8))
            .unwrap();
        reference
            .append(ConstructionChunkKind::Node, "nodes-2", &node_batch(9, 8))
            .unwrap();
        reference.seal().unwrap();
        reference
            .shape_canonical_with_cancellation(|| false)
            .unwrap();
        let expected = reference.evidence().clone();

        for failpoint in [
            "shape.fixed.after_install",
            "shape.fixed_merge.after_install",
            "shape.row_merge.after_install",
            "shape.after_complete_inventory",
            "shape.after_evidence_checkpoint",
        ] {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("graph_construction::tests::crash_subprocess_helper")
                .arg("--nocapture")
                .env("GF_CONSTRUCTION_CRASH_ROOT", root.path())
                .env("GF_CONSTRUCTION_SHAPE_CRASH", "1")
                .env(
                    "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                    "graphforge-construction-test-v1",
                )
                .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");
            let mut resumed = GraphConstructionSession::open(
                root.path(),
                Uuid::from_u128(600),
                0,
                GraphConstructionBudgets::default(),
            )
            .unwrap();
            resumed.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(resumed.evidence(), &expected, "{failpoint}");
        }
    }

    #[test]
    fn canonical_encoder_outputs_feed_ordinary_readers_index_and_adjacency() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_320);
        let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
        let route = |kind, local: &str| {
            authority
                .bindings
                .bindings
                .iter()
                .find(|binding| binding.route_kind == kind && binding.symbol.local_id == local)
                .unwrap()
                .route
                .clone()
        };
        let relation_route = route(crate::SemanticRouteKind::Relation, "R");
        let node_property_route = route(crate::SemanticRouteKind::NodeProperty, "Person:score");
        let edge_property_route = route(crate::SemanticRouteKind::EdgeProperty, "R:weight");
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            authority,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes-a",
                &node_property_batch(1, 2),
            )
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes-b",
                &node_property_batch(3, 1),
            )
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Edge,
                "edges",
                &edge_property_batch(100, 2),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.ontology_mode, graphforge_core::OntologyMode::Strict);
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        assert_eq!(encoding.evidence.prior_topology_rows_decoded, 0);
        assert_eq!(encoding.evidence.retained_topology_bytes_copied, 0);
        assert_eq!(encoding.evidence.membership_records, 5);
        assert_eq!(encoding.evidence.ordinal_records, 3);
        assert_eq!(encoding.evidence.ordinal_artifact_write_bytes, 120);
        assert_eq!(encoding.evidence.ordinal_artifact_write_operations, 2);
        assert_eq!(encoding.evidence.ordinal_ranges, 1);
        assert_eq!(encoding.evidence.ordinal_work_operations, 3);
        assert_eq!(encoding.evidence.ordinal_peak_buffer_bytes, 3 * 64 * 1024);
        assert_eq!(encoding.evidence.ordinal_publication_write_operations, 3);
        // Three artifact file syncs, one artifact-directory barrier, two
        // barriers for each of receipt/manifest/lock, and four ancestor
        // directory barriers after the complete facet is installed.
        assert_eq!(encoding.evidence.ordinal_fsync_operations, 14);
        let ordinal_publication_bytes = encoding
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.path.ends_with("ordinal-v4-receipt.json")
                    || artifact.path.ends_with("ordinal-v4-manifest.json")
                    || artifact.path.ends_with("ordinal-v4.lock")
            })
            .map(|artifact| artifact.bytes)
            .sum::<u64>();
        assert_eq!(
            encoding.evidence.ordinal_publication_write_bytes,
            ordinal_publication_bytes
        );
        assert_eq!(
            encoding.evidence.ordinal_peak_temporary_bytes,
            encoding
                .evidence
                .ordinal_artifact_write_bytes
                .saturating_add(ordinal_publication_bytes)
        );

        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoding.root)
            .join("graph");
        let nodes = crate::read_nodes(&graph).unwrap();
        assert_eq!(nodes.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let edges = crate::read_edges(
            &graph,
            &relation_route,
            graphforge_core::OntologyMode::Strict,
        )
        .unwrap();
        assert_eq!(edges.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let node_properties = crate::read_properties(&graph, &node_property_route).unwrap();
        assert_eq!(
            node_properties
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        let edge_properties = crate::read_edge_properties(&graph, &edge_property_route).unwrap();
        assert_eq!(
            edge_properties
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        let index = crate::UuidMembershipIndex::open(&graph).unwrap();
        assert_eq!(index.count(crate::UuidIndexKind::Node), 3);
        assert_eq!(index.count(crate::UuidIndexKind::Edge), 2);
        let adjacency =
            crate::adjacency::build_adjacency_index(&graph, shape.runtime_catalog_now_micros)
                .unwrap();
        assert!(!adjacency.is_empty());

        let resumed = session.encode_canonical(&shape, 1).unwrap();
        assert_eq!(resumed, encoding);
    }

    #[test]
    fn fresh_construction_publishes_selected_authenticated_v4_exact_lookup() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_321);
        let target = Uuid::from_u128(9_322);
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 3))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        session
            .publish_canonical(&encoding, target, Uuid::from_u128(9_323))
            .unwrap();

        let selected = crate::resolve_project_generation(root.path()).unwrap();
        assert_eq!(selected.generation_uuid(), target);
        let authority = selected
            .authenticated_v4_ordinal_authority()
            .unwrap()
            .expect("fresh construction publishes v4 authority");
        let inventory = selected.graph_files_inventory().unwrap().unwrap();
        let materialized = TempDir::new().unwrap();
        let graph = materialized.path().join("graph");
        std::fs::create_dir(&graph).unwrap();
        crate::materialize_graph_objects(root.path(), &inventory, &graph).unwrap();
        let mut handle = match authority
            .open(&graph, crate::V4OrdinalIdentityLimits::default())
            .unwrap()
        {
            crate::V4OrdinalIdentityOpen::Ready(handle) => handle,
            crate::V4OrdinalIdentityOpen::RebuildRequired { found_version } => {
                panic!("fresh v4 unexpectedly requires rebuild from {found_version}")
            }
        };
        let lookup = handle.lookup_node_uuids(&[3, 1, 4, 2]).unwrap();
        assert_eq!(
            lookup.values,
            vec![
                Some(Uuid::from_u128(3)),
                Some(Uuid::from_u128(1)),
                None,
                Some(Uuid::from_u128(2)),
            ]
        );
    }

    #[test]
    fn canonical_encoder_merges_bounded_heterogeneous_node_schemas() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_323);
        let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
        let property_route = authority
            .bindings
            .bindings
            .iter()
            .find(|binding| {
                binding.route_kind == crate::SemanticRouteKind::NodeProperty
                    && binding.symbol.local_id == "Person:score"
            })
            .unwrap()
            .route
            .clone();
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            authority,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "plain", &node_batch(1, 1))
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "property",
                &node_property_batch(2, 1),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.node_rows.len(), 2);
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        assert_eq!(
            crate::read_nodes(&graph)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            crate::read_properties(&graph, &property_route)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert!(encoded.evidence.peak_batch_rows <= 65_536);
        assert!(encoded.evidence.input_read_operations > 0);
        assert!(encoded.evidence.output_write_operations > 0);
        assert_eq!(encoded.evidence.peak_open_input_readers, 1);
    }

    #[test]
    fn canonical_encoder_routes_inherited_property_by_declaring_owner() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_324);
        let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
        let property_route = authority
            .bindings
            .bindings
            .iter()
            .find(|binding| {
                binding.route_kind == crate::SemanticRouteKind::NodeProperty
                    && binding.symbol.local_id == "Person:score"
            })
            .unwrap()
            .route
            .clone();
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            authority,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "child",
                &node_property_batch_for(1, 2, "Child"),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        assert_eq!(
            crate::read_properties(&graph, &property_route)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn canonical_encoder_splits_child_and_inherited_properties_by_declaring_route() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_326);
        let authority = semantic_authority(graphforge_core::OntologyMode::Strict);
        let binding_route = |kind, local: &str| {
            authority
                .bindings
                .bindings
                .iter()
                .find(|binding| binding.route_kind == kind && binding.symbol.local_id == local)
                .unwrap()
                .route
                .clone()
        };
        let parent_route = binding_route(crate::SemanticRouteKind::NodeProperty, "Person:score");
        let child_route = binding_route(crate::SemanticRouteKind::NodeProperty, "Child:nickname");
        let concrete_route = binding_route(crate::SemanticRouteKind::Entity, "Child");
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            authority,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "child-parent-properties",
                &child_parent_property_batch(1, 2),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        for (route, property) in [(parent_route, "score"), (child_route, "nickname")] {
            let batches = crate::read_properties(&graph, &route).unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.column_by_name(property).is_some())
            );
            assert!(batches.iter().all(|batch| {
                batch.schema().metadata().get("graphforge.entity_type") == Some(&concrete_route)
            }));
        }
    }

    #[test]
    fn canonical_encoder_keeps_same_local_property_names_module_qualified() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_327);
        let authority = colliding_module_authority();
        let mut labels = authority
            .bindings
            .bindings
            .iter()
            .filter(|binding| binding.route_kind == crate::SemanticRouteKind::Entity)
            .map(|binding| binding.symbol.ambiguity_candidate())
            .collect::<Vec<_>>();
        labels.sort();
        let property_routes = authority
            .bindings
            .bindings
            .iter()
            .filter(|binding| binding.route_kind == crate::SemanticRouteKind::NodeProperty)
            .map(|binding| binding.route.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(property_routes.len(), 2);
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            authority,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        for (index, label) in labels.iter().enumerate() {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("module-{index}"),
                    &colliding_property_batch(index as u128 + 1, label, index as i64),
                )
                .unwrap();
        }
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        for route in property_routes {
            assert_eq!(
                crate::read_properties(&graph, &route)
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
        }
    }

    #[test]
    fn canonical_encoder_keeps_256_heterogeneous_schemas_to_one_live_reader() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_325);
        let budgets = GraphConstructionBudgets {
            max_batch_rows: 1,
            max_run_records: 4,
            ..GraphConstructionBudgets::default()
        };
        let mut session = GraphConstructionSession::open_with_mode(
            root.path(),
            operation,
            0,
            graphforge_core::OntologyMode::Exploratory,
            budgets,
        )
        .unwrap();
        for index in 0..256 {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("schema-{index:03}"),
                    &heterogeneous_property_batch(index as u128 + 1, index),
                )
                .unwrap();
        }
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.node_rows.len(), 256);
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        assert_eq!(encoded.evidence.peak_open_input_readers, 1);
        assert!(encoded.evidence.peak_batch_rows <= 1);
        let graph = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        assert_eq!(
            crate::read_nodes(&graph)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            256
        );
    }

    #[test]
    fn canonical_encoder_cancellation_recovers_and_corruption_fails_closed() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_322);
        let mut session = GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            semantic_authority(graphforge_core::OntologyMode::Strict),
            GraphConstructionBudgets {
                max_batch_rows: 2,
                ..GraphConstructionBudgets::default()
            },
        )
        .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes",
                &node_property_batch(1, 2),
            )
            .unwrap();
        session
            .append(
                ConstructionChunkKind::Node,
                "nodes-2",
                &node_property_batch(3, 1),
            )
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let mut polls = 0;
        let error = session
            .encode_canonical_with_cancellation(&shape, 1, || {
                polls += 1;
                polls == 3
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        let uuid_private = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join("encoded-v1/graph/topology/uuid-membership");
        if uuid_private.exists() {
            assert_eq!(std::fs::read_dir(&uuid_private).unwrap().count(), 0);
        }

        let encoded = session.encode_canonical(&shape, 1).unwrap();
        let operation_root = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root);
        let victim = encoded
            .artifacts
            .iter()
            .find(|artifact| artifact.path.starts_with("topology/nodes/"))
            .unwrap();
        std::fs::write(operation_root.join("graph").join(&victim.path), b"corrupt").unwrap();
        let error = session.encode_canonical(&shape, 1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("canonical artifact differs from inventory")
        );
    }

    #[test]
    fn canonical_encoder_rejects_same_inode_mutate_restore_during_spool() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_328);
        let mut session = open(&root, 9_328);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let source = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&shape.runtime_catalog);
        let original = std::fs::read(&source).unwrap()[0];
        let mut mutated = false;
        let hook_source = source.clone();
        crate::graph_construction_encoding::set_source_spool_hook(Some(Box::new(move |phase| {
            use std::io::{Seek as _, SeekFrom};
            if phase == "before_read" && !mutated {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&hook_source)
                    .unwrap();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&[original ^ 0xff]).unwrap();
                mutated = true;
            } else if phase == "after_read" && mutated {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&hook_source)
                    .unwrap();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&[original]).unwrap();
            }
        })));
        let error = session.encode_canonical(&shape, 1).unwrap_err();
        crate::graph_construction_encoding::set_source_spool_hook(None);
        assert!(
            error
                .to_string()
                .contains("changed during authenticated spooling")
        );
        assert_eq!(std::fs::read(source).unwrap()[0], original);
    }

    #[test]
    fn canonical_encoder_uses_owned_spool_after_source_mutation() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_329);
        let mut session = open(&root, 9_329);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let source = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&shape.runtime_catalog);
        let original = std::fs::read(&source).unwrap()[0];
        let hook_source = source.clone();
        let mut changed = false;
        crate::graph_construction_encoding::set_source_spool_hook(Some(Box::new(move |phase| {
            if phase == "after_read" && !changed {
                use std::io::{Seek as _, SeekFrom};
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&hook_source)
                    .unwrap();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&[original ^ 0xff]).unwrap();
                changed = true;
            }
        })));
        let encoded = session.encode_canonical(&shape, 1).unwrap();
        crate::graph_construction_encoding::set_source_spool_hook(None);
        assert!(encoded.evidence.source_spool_read_bytes > 0);
        assert_ne!(std::fs::read(source).unwrap()[0], original);
    }

    #[test]
    fn canonical_encoder_rejects_coherent_inventory_and_artifact_substitution() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_330);
        let mut session = open(&root, 9_330);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let mut encoded = session.encode_canonical(&shape, 1).unwrap();
        let encoded_root = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root);
        let artifact = encoded
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.path == "topology/surrogate_tails.parquet")
            .unwrap();
        let artifact_path = encoded_root.join("graph").join(&artifact.path);
        let mut body = std::fs::read(&artifact_path).unwrap();
        let last = body.len() - 1;
        body[last] ^= 0xff;
        std::fs::write(&artifact_path, &body).unwrap();
        artifact.sha256 = sha256(&body);
        std::fs::write(
            encoded_root.join("inventory.json"),
            serde_json::to_vec(&encoded).unwrap(),
        )
        .unwrap();
        let error = session.encode_canonical(&shape, 1).unwrap_err();
        assert!(error.to_string().contains("checkpoint inventory authority"));
    }

    #[test]
    fn construction_checkpoint_rejects_ontology_mode_change_on_resume() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_321);
        drop(
            GraphConstructionSession::open_with_semantic_authority(
                root.path(),
                operation,
                0,
                semantic_authority(graphforge_core::OntologyMode::Advisory),
                GraphConstructionBudgets::default(),
            )
            .unwrap(),
        );
        let error = match GraphConstructionSession::open_with_semantic_authority(
            root.path(),
            operation,
            0,
            semantic_authority(graphforge_core::OntologyMode::Strict),
            GraphConstructionBudgets::default(),
        ) {
            Ok(_) => panic!("ontology mode mismatch was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("resume parameters changed"));
    }

    #[test]
    fn canonical_routing_is_checkpoint_bound_in_all_ontology_modes() {
        for (offset, mode) in [
            (0_u128, graphforge_core::OntologyMode::Exploratory),
            (1, graphforge_core::OntologyMode::Advisory),
            (2, graphforge_core::OntologyMode::Strict),
        ] {
            let root = TempDir::new().unwrap();
            let operation = Uuid::from_u128(9_330 + offset);
            let authority = (mode != graphforge_core::OntologyMode::Exploratory)
                .then(|| semantic_authority(mode));
            let mut session = if mode == graphforge_core::OntologyMode::Exploratory {
                GraphConstructionSession::open_with_mode(
                    root.path(),
                    operation,
                    0,
                    mode,
                    GraphConstructionBudgets::default(),
                )
                .unwrap()
            } else {
                GraphConstructionSession::open_with_semantic_authority(
                    root.path(),
                    operation,
                    0,
                    authority.clone().unwrap(),
                    GraphConstructionBudgets::default(),
                )
                .unwrap()
            };
            session
                .append(
                    ConstructionChunkKind::Node,
                    "nodes",
                    &node_property_batch(1, 2),
                )
                .unwrap();
            session
                .append(
                    ConstructionChunkKind::Edge,
                    "edges",
                    &edge_property_batch(100, 1),
                )
                .unwrap();
            session.seal().unwrap();
            let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
            assert_eq!(shape.ontology_mode, mode);
            let encoding = session.encode_canonical(&shape, 1).unwrap();
            let graph = root
                .path()
                .join(PRIVATE_ROOT)
                .join(operation.simple().to_string())
                .join(&encoding.root)
                .join("graph");
            let route = |kind, local: &str, fallback: &str| {
                authority
                    .as_ref()
                    .and_then(|authority| {
                        authority.bindings.bindings.iter().find(|binding| {
                            binding.route_kind == kind && binding.symbol.local_id == local
                        })
                    })
                    .map_or_else(|| fallback.to_owned(), |binding| binding.route.clone())
            };
            let relation_route = route(crate::SemanticRouteKind::Relation, "R", "R");
            assert_eq!(
                crate::read_edges(&graph, &relation_route, mode)
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
            let property_stem = if mode == graphforge_core::OntologyMode::Exploratory {
                assert!(graph.join("topology/edges/_exploratory").is_dir());
                "_untyped".to_owned()
            } else {
                assert!(!graph.join("topology/edges/R").exists());
                route(
                    crate::SemanticRouteKind::NodeProperty,
                    "Person:score",
                    "Person",
                )
            };
            assert_eq!(
                crate::read_properties(&graph, &property_stem)
                    .unwrap()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                2
            );
            assert_eq!(
                crate::read_edge_properties(
                    &graph,
                    &route(
                        crate::SemanticRouteKind::EdgeProperty,
                        "R:weight",
                        "_exploratory",
                    ),
                )
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
                1
            );
        }
    }

    #[test]
    fn generation_two_parent_index_is_structurally_referenced_without_payload_copy() {
        let project = nonempty_project_generation_two();
        let operation = Uuid::from_u128(9_340);
        let mut session = GraphConstructionSession::open_with_mode(
            project.path(),
            operation,
            2,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "delta", &node_batch(4, 1))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = session.encode_canonical(&shape, 3).unwrap();
        assert_eq!(encoded.evidence.retained_index_payload_bytes, 0);
        assert_eq!(encoded.evidence.retained_topology_bytes_copied, 0);
        assert_eq!(encoded.evidence.prior_topology_rows_decoded, 0);
        assert_eq!(encoded.evidence.retained_index_runs, 2);
        let index_outputs = encoded
            .artifacts
            .iter()
            .filter(|artifact| artifact.path.contains("uuid-membership"))
            .count();
        // New identity + reverse runs and the new manifest. The retained base
        // and level-one descriptors remain structural references.
        assert_eq!(index_outputs, 3);
        assert!(!encoded.retained_artifacts.is_empty());
        let assembled = project
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        for retained in &encoded.retained_artifacts {
            let target = assembled.join(&retained.target_path);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::hard_link(
                std::path::Path::new(&retained.source_root).join(&retained.source_path),
                target,
            )
            .unwrap();
        }
        let opened = crate::UuidMembershipIndex::open(&assembled).unwrap();
        assert_eq!(opened.count(crate::UuidIndexKind::Node), 4);
    }

    #[test]
    fn completed_encoding_replay_reauthenticates_retained_parent_payload() {
        let project = nonempty_project_generation_two();
        let operation = Uuid::from_u128(9_343);
        let mut session = GraphConstructionSession::open_with_mode(
            project.path(),
            operation,
            2,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "delta", &node_batch(4, 1))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = session.encode_canonical(&shape, 3).unwrap();
        let retained = encoded.retained_artifacts.first().unwrap();
        let path = std::path::Path::new(&retained.source_root).join(&retained.source_path);
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        use std::io::{Seek as _, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        let error = session.encode_canonical(&shape, 3).unwrap_err();
        assert!(error.to_string().contains("retained construction"));
    }

    #[test]
    fn generation_one_parent_uses_streamed_binary_carry_and_authenticates_result() {
        let project = nonempty_project_with_nodes(2);
        let operation = Uuid::from_u128(9_341);
        let mut session = GraphConstructionSession::open_with_mode(
            project.path(),
            operation,
            1,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "delta", &node_batch(3, 1))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoded = session.encode_canonical(&shape, 2).unwrap();
        assert!(encoded.evidence.retained_index_payload_bytes > 0);
        assert!(encoded.evidence.membership_read_bytes > 0);
        assert!(
            encoded.evidence.membership_total_write_bytes > encoded.evidence.membership_write_bytes
        );

        let graph = project
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoded.root)
            .join("graph");
        let parent_index = project.path().join("topology/uuid-membership");
        let encoded_index = graph.join("topology/uuid-membership");
        let parent_manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(parent_index.join("manifest.json")).unwrap())
                .unwrap();
        for run in parent_manifest["runs"].as_array().unwrap() {
            if !run["base"].as_bool().unwrap() {
                continue;
            }
            for field in ["identities", "node_surrogates"] {
                let name = run[field]["name"].as_str().unwrap();
                assert_eq!(std::fs::metadata(parent_index.join(name)).unwrap().len(), 0);
                std::fs::copy(parent_index.join(name), encoded_index.join(name)).unwrap();
            }
        }
        let index = crate::UuidMembershipIndex::open(&graph).unwrap();
        assert_eq!(index.count(crate::UuidIndexKind::Node), 3);
        assert_eq!(index.count(crate::UuidIndexKind::Edge), 1);
    }

    #[test]
    fn parent_uuid_path_substitution_is_rejected_before_encoding() {
        let project = nonempty_project_with_nodes(2);
        let operation = Uuid::from_u128(9_342);
        let mut session = GraphConstructionSession::open_with_mode(
            project.path(),
            operation,
            1,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "delta", &node_batch(3, 1))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let membership = project.path().join("topology/uuid-membership");
        let victim = std::fs::read_dir(&membership)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "uuidx")
            })
            .unwrap();
        let saved = victim.with_extension("uuidx.saved");
        std::fs::rename(&victim, &saved).unwrap();
        std::fs::copy(&saved, &victim).unwrap();
        let error = session.encode_canonical(&shape, 2).unwrap_err();
        assert!(error.to_string().contains("identity changed"));
    }

    fn encoded_publication_session(root: &TempDir, operation: Uuid) -> GraphConstructionSession {
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        session.encode_canonical(&shape, 1).unwrap();
        session
    }

    fn publish_empty_generation(
        root: &TempDir,
        target: Uuid,
        transaction: Uuid,
    ) -> crate::ProjectPublicationReceipt {
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: transaction,
            generation_uuid: target,
            capabilities: vec![crate::ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            }],
            participants: vec![],
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new publication unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap()
    }

    #[test]
    fn publication_state_is_idempotent_and_rejects_changed_target() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_400);
        let target = Uuid::from_u128(9_401);
        let transaction = Uuid::from_u128(9_402);
        let mut session = encoded_publication_session(&root, operation);
        let first = session.begin_publication(target, transaction).unwrap();
        assert_eq!(
            session.checkpoint.publication_state,
            Some(ConstructionPublicationState::Publishing)
        );
        assert_eq!(
            session.begin_publication(target, transaction).unwrap(),
            first
        );
        assert!(
            session
                .begin_publication(Uuid::from_u128(9_403), transaction)
                .unwrap_err()
                .to_string()
                .contains("target changed")
        );
        let published = publish_empty_generation(&root, target, transaction);
        let digest = hex(&published.generation_manifest_sha256);
        let receipt = session.finish_publication(target, &digest).unwrap();
        assert_eq!(
            session.checkpoint.publication_state,
            Some(ConstructionPublicationState::Published)
        );
        assert_eq!(
            session.finish_publication(target, &digest).unwrap(),
            receipt
        );
        assert!(
            session
                .finish_publication(target, &"cd".repeat(32))
                .unwrap_err()
                .to_string()
                .contains("result changed")
        );
    }

    #[test]
    fn canonical_publication_installs_compact_graph_and_advances_current_once() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let operation = Uuid::from_u128(9_450);
        let target = Uuid::from_u128(9_451);
        let transaction = Uuid::from_u128(9_452);
        let mut session = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();

        let receipt = session
            .publish_canonical(&encoding, target, transaction)
            .unwrap();
        assert_eq!(receipt.generation_uuid, target);
        assert!(!receipt.idempotent_replay);
        let current = crate::resolve_project_generation(root.path()).unwrap();
        assert_eq!(current.generation_uuid(), target);
        let inventory = current.graph_files_inventory().unwrap().unwrap();
        assert!(
            inventory
                .files
                .iter()
                .any(|entry| entry.relative_path == "topology/generation.json")
        );
        assert!(
            inventory
                .files
                .iter()
                .any(|entry| entry.relative_path.starts_with("topology/nodes/"))
        );
        assert!(
            inventory
                .files
                .iter()
                .any(|entry| { entry.relative_path == "topology/uuid-membership/manifest.json" })
        );
        let current_path = root.path().join("CURRENT");
        let current_bytes = std::fs::read(&current_path).unwrap();
        std::fs::write(&current_path, b"concurrently-advanced-current\n").unwrap();
        assert_eq!(
            compact_parent_surrogate_tails(root.path(), &inventory).unwrap(),
            Some((2, 0)),
            "pinned compact-parent tails must not consult mutable CURRENT"
        );
        std::fs::write(&current_path, current_bytes).unwrap();
        let materialized = TempDir::new().unwrap();
        let materialized_graph = materialized.path().join("graph");
        std::fs::create_dir(&materialized_graph).unwrap();
        crate::materialize_graph_objects(root.path(), &inventory, &materialized_graph).unwrap();
        let uuid_index = crate::UuidMembershipIndex::open(&materialized_graph).unwrap();
        assert_eq!(uuid_index.count(crate::UuidIndexKind::Node), 2);
        assert_eq!(uuid_index.count(crate::UuidIndexKind::Edge), 0);
        drop(current);
        let receipt_path = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(PUBLICATION_RECEIPT);
        std::fs::remove_file(receipt_path).unwrap();
        session.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
        replace_control(&session.root, CHECKPOINT, &session.checkpoint).unwrap();
        drop(session);

        let mut resumed = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        let replay = resumed
            .publish_canonical(&encoding, target, transaction)
            .unwrap();
        assert_eq!(replay.generation_uuid, target);
        assert_eq!(
            crate::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            target
        );
    }

    #[test]
    fn canonical_publication_rejects_tampered_artifact_before_current() {
        let root = TempDir::new().unwrap();
        let parent = crate::open_or_initialize_project(root.path()).unwrap();
        let prior = parent.generation_uuid();
        drop(parent);
        let operation = Uuid::from_u128(9_460);
        let mut session = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        let victim = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(&encoding.root)
            .join("graph")
            .join(&encoding.artifacts[0].path);
        std::fs::write(victim, b"tampered").unwrap();

        let error = session
            .publish_canonical(&encoding, Uuid::from_u128(9_461), Uuid::from_u128(9_462))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("authenticated graph file metadata changed")
                || error.contains("digest or length changed")
                || error.contains("graph object source is not the declared regular file"),
            "unexpected corruption error: {error}"
        );
        assert_eq!(
            crate::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            prior
        );
        assert_eq!(
            session.checkpoint.publication_state,
            Some(ConstructionPublicationState::Sealed)
        );
    }

    #[test]
    fn canonical_publication_rejects_replaced_durable_inventory_without_payload_reads() {
        let root = TempDir::new().unwrap();
        let parent = crate::open_or_initialize_project(root.path()).unwrap();
        let prior = parent.generation_uuid();
        drop(parent);
        let operation = Uuid::from_u128(9_468);
        let mut session = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 1))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        let inventory_path = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join("encoded-v1/inventory.json");
        let mut replaced: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&inventory_path).unwrap()).unwrap();
        replaced["generation"] = serde_json::json!(2);
        std::fs::write(&inventory_path, serde_json::to_vec(&replaced).unwrap()).unwrap();

        let error = session
            .publish_canonical(&encoding, Uuid::from_u128(9_469), Uuid::from_u128(9_470))
            .unwrap_err();
        assert!(error.to_string().contains("durable encoding"));
        assert_eq!(
            crate::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            prior
        );
        assert_eq!(
            session.checkpoint.publication_state,
            Some(ConstructionPublicationState::Sealed)
        );
    }

    #[test]
    fn canonical_publication_cancels_at_named_immediate_pre_current_boundary() {
        let root = TempDir::new().unwrap();
        let parent = crate::open_or_initialize_project(root.path()).unwrap();
        let prior = parent.generation_uuid();
        drop(parent);
        let prior_current = std::fs::read(root.path().join("CURRENT")).unwrap();
        let mut session = GraphConstructionSession::open(
            root.path(),
            Uuid::from_u128(9_465),
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        let target = Uuid::from_u128(9_466);
        let transaction = Uuid::from_u128(9_467);
        let mut checkpoints = 0_u8;
        let error = session
            .publish_canonical_with_cancellation(&encoding, target, transaction, || {
                checkpoints += 1;
                checkpoints == 2
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
        assert!(error.to_string().contains("before_current_replace"));
        assert_eq!(
            checkpoints, 2,
            "entry and immediate pre-CURRENT checkpoints"
        );
        assert_eq!(
            std::fs::read(root.path().join("CURRENT")).unwrap(),
            prior_current
        );
        assert_ne!(target, prior);
        assert_eq!(
            session.checkpoint.publication_state,
            Some(ConstructionPublicationState::Publishing),
            "the durable intent remains recoverable without claiming commit"
        );
    }

    #[test]
    fn project_container_parent_binding_uses_exact_uuid_and_manifest() {
        let root = TempDir::new().unwrap();
        let parent = crate::open_or_initialize_project(root.path()).unwrap();
        let expected = (parent.generation_uuid(), hex(&parent.manifest_sha256()));
        drop(parent);
        assert_eq!(
            current_parent_generation_authority(root.path()).unwrap(),
            expected
        );
    }

    #[test]
    fn publication_reopen_recovers_each_durable_crash_boundary() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_410);
        let target = Uuid::from_u128(9_411);
        let transaction = Uuid::from_u128(9_412);
        let mut session = encoded_publication_session(&root, operation);
        session.begin_publication(target, transaction).unwrap();
        session.checkpoint.publication_state = Some(ConstructionPublicationState::Sealed);
        replace_control(&session.root, CHECKPOINT, &session.checkpoint).unwrap();
        drop(session);
        let published = publish_empty_generation(&root, target, transaction);
        publish_empty_generation(&root, Uuid::from_u128(9_413), Uuid::from_u128(9_414));
        let mut reopened = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(
            reopened.checkpoint.publication_state,
            Some(ConstructionPublicationState::Publishing)
        );
        reopened
            .finish_publication(target, &hex(&published.generation_manifest_sha256))
            .unwrap();
        reopened.checkpoint.publication_state = Some(ConstructionPublicationState::Publishing);
        replace_control(&reopened.root, CHECKPOINT, &reopened.checkpoint).unwrap();
        drop(reopened);
        let reopened = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(
            reopened.checkpoint.publication_state,
            Some(ConstructionPublicationState::Published)
        );
    }

    #[test]
    fn publication_reopen_rejects_corrupt_or_mismatched_authority() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_420);
        let mut session = encoded_publication_session(&root, operation);
        session
            .begin_publication(Uuid::from_u128(9_421), Uuid::from_u128(9_422))
            .unwrap();
        let intent_path = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(PUBLICATION_INTENT);
        let mut intent: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();
        intent["parent_generation_manifest_sha256"] = serde_json::Value::String("00".repeat(32));
        std::fs::write(&intent_path, serde_json::to_vec(&intent).unwrap()).unwrap();
        drop(session);
        let error = match GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        ) {
            Ok(_) => panic!("corrupt publication intent was accepted"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("publication intent authority changed")
        );
    }

    #[test]
    fn publication_finish_rejects_wrong_target_and_parent() {
        let wrong_target_root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_430);
        let intended = Uuid::from_u128(9_431);
        let transaction = Uuid::from_u128(9_432);
        let mut session = encoded_publication_session(&wrong_target_root, operation);
        session.begin_publication(intended, transaction).unwrap();
        let other = publish_empty_generation(
            &wrong_target_root,
            Uuid::from_u128(9_433),
            Uuid::from_u128(9_434),
        );
        assert!(
            session
                .finish_publication(intended, &hex(&other.generation_manifest_sha256))
                .unwrap_err()
                .to_string()
                .contains("target cannot be authenticated")
        );

        let wrong_parent_root = TempDir::new().unwrap();
        let mut session = encoded_publication_session(&wrong_parent_root, Uuid::from_u128(9_440));
        let first = publish_empty_generation(
            &wrong_parent_root,
            Uuid::from_u128(9_441),
            Uuid::from_u128(9_442),
        );
        let target = Uuid::from_u128(9_443);
        let transaction = Uuid::from_u128(9_444);
        session.begin_publication(target, transaction).unwrap();
        let second = publish_empty_generation(&wrong_parent_root, target, transaction);
        assert_ne!(first.generation_uuid, second.generation_uuid);
        assert!(
            session
                .finish_publication(target, &hex(&second.generation_manifest_sha256))
                .unwrap_err()
                .to_string()
                .contains("not a child of the pinned parent")
        );
    }

    #[test]
    fn published_receipt_reopens_after_later_current_advances() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_450);
        let mut session = encoded_publication_session(&root, operation);
        let target = Uuid::from_u128(9_451);
        let transaction = Uuid::from_u128(9_452);
        session.begin_publication(target, transaction).unwrap();
        let target_receipt = publish_empty_generation(&root, target, transaction);
        let construction_receipt = session
            .finish_publication(target, &hex(&target_receipt.generation_manifest_sha256))
            .unwrap();
        drop(session);
        publish_empty_generation(&root, Uuid::from_u128(9_453), Uuid::from_u128(9_454));
        let mut reopened = GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        )
        .unwrap();
        assert_eq!(
            reopened
                .finish_publication(target, &hex(&target_receipt.generation_manifest_sha256))
                .unwrap(),
            construction_receipt
        );
    }

    #[test]
    fn persisted_authority_digests_reject_uppercase_hex() {
        let root = TempDir::new().unwrap();
        let operation = Uuid::from_u128(9_460);
        let target = Uuid::from_u128(9_461);
        let transaction = Uuid::from_u128(9_462);
        let mut session = encoded_publication_session(&root, operation);
        session.begin_publication(target, transaction).unwrap();
        let published = publish_empty_generation(&root, target, transaction);
        assert!(
            session
                .finish_publication(
                    target,
                    &hex(&published.generation_manifest_sha256).to_ascii_uppercase(),
                )
                .unwrap_err()
                .to_string()
                .contains("digest is invalid")
        );

        let intent_path = root
            .path()
            .join(PRIVATE_ROOT)
            .join(operation.simple().to_string())
            .join(PUBLICATION_INTENT);
        let mut intent: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();
        intent["shape_authority_sha256"] = serde_json::Value::String(
            intent["shape_authority_sha256"]
                .as_str()
                .unwrap()
                .to_ascii_uppercase(),
        );
        std::fs::write(&intent_path, serde_json::to_vec(&intent).unwrap()).unwrap();
        drop(session);
        let error = match GraphConstructionSession::open(
            root.path(),
            operation,
            0,
            GraphConstructionBudgets::default(),
        ) {
            Ok(_) => panic!("uppercase persisted authority was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("digest is invalid"));
    }
}
