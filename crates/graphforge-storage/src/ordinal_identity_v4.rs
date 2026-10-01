//! Read-only authenticated v4 `node_id -> node_uuid` authority.
//!
//! Opening reads no artifact byte; every ordinal and tombstone block is
//! authenticated by the lookup that reads it (#1388).
//!
//! This module deliberately exposes no publication API. Version-three state is
//! reported as rebuild-required and is never interpreted through this format.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path};
use std::time::SystemTime;

use graphforge_core::hash_observation::ControlSha256 as Sha256;
use graphforge_filesystem::{FileIdentity, StableDirectory, file_identity, file_link_count};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use uuid::Uuid;

/// Checksum-bearing wire version of the ordinal mapping authority.
pub const ORDINAL_IDENTITY_V4: u32 = 6;
/// Canonical location below a graph project.
pub const ORDINAL_IDENTITY_MANIFEST: &str = "topology/uuid-membership/ordinal-v4-manifest.json";
const INDEX_DIR: &str = "topology/uuid-membership";
const MANIFEST_NAME: &str = "ordinal-v4-manifest.json";
const LOCK_NAME: &str = "ordinal-v4.lock";
const RECEIPT_FILE_NAME: &str = "ordinal-v4-receipt.json";
const RECEIPT_NAME: &str = "topology/uuid-membership/ordinal-v4-receipt.json";
const GENERATION_NAME: &str = "topology/generation.json";
const UUID_WIDTH: u64 = 16;
const FORWARD_RECORD_WIDTH: u64 = UUID_WIDTH + 8;
const FORWARD_RECORD_WIDTH_USIZE: usize = UUID_WIDTH_USIZE + 8;
const UUID_WIDTH_USIZE: usize = 16;
const TOMBSTONE_WIDTH: u64 = 8;
const TOMBSTONE_WIDTH_USIZE: usize = 8;
const TOMBSTONE_BLOCK_BYTES: u64 = 64 * 1024;
/// Bytes in one authenticated ordinal block, the unit a lookup reads.
pub const ORDINAL_BLOCK_BYTES: u64 = 64 * 1024;
/// UUIDs in one full ordinal block.
pub const ORDINAL_BLOCK_RECORDS: u64 = ORDINAL_BLOCK_BYTES / UUID_WIDTH;
const ORDINAL_BLOCK_BYTES_USIZE: usize = 64 * 1024;
pub(crate) const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const STREAM_BYTES: usize = 1024 * 1024;
// BTree nodes, sorted request storage, caller-order output, and resolved-map
// entries are charged conservatively rather than as payload-only scalars.
const REQUEST_ENTRY_CHARGE: u64 = 128;
// Empty BTreeMap + VecDeque + accounting field retained in the handle.
const TOMBSTONE_CACHE_FIXED_CHARGE: usize = 64;
const TOMBSTONE_CACHE_ENTRY_CHARGE: usize = 192;
const DESCRIPTOR_FIXED_CHARGE: usize = 512;
const ARTIFACT_DESCRIPTOR_CHARGE: u64 = 256;
const ORDINAL_BLOCK_DESCRIPTOR_CHARGE: u64 = 192;
const TOMBSTONE_BLOCK_DESCRIPTOR_CHARGE: u64 = 224;
const ADMISSION_TRANSIENT_FIXED_CHARGE: u64 = 256;

/// Hard anonymous-memory and read-coalescing limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V4OrdinalIdentityLimits {
    /// Maximum caller entries, including duplicates, in one lookup.
    pub max_requested: usize,
    /// Maximum byte gap folded into one range read.
    pub coalesce_gap_bytes: u64,
    /// Maximum allocation and payload size for one coalesced read.
    pub max_coalesced_read_bytes: usize,
    /// Total charged capacity retained by the handle-global tombstone cache.
    pub max_tombstone_cache_bytes: usize,
    /// Bytes of already-authenticated ordinal blocks the handle retains so a
    /// block is read from disk once per handle, not once per lookup. Zero
    /// disables the cache. A handle keeps serving blocks it already
    /// authenticated after a later same-inode flip of the artifact; a fresh
    /// handle refuses the flipped block. Nothing that was wrong when it was
    /// read is ever returned. Crate-private: no caller outside storage tunes it.
    pub(crate) max_ordinal_cache_bytes: usize,
    /// Maximum conservatively charged retained manifest/descriptor metadata.
    pub max_descriptor_metadata_bytes: usize,
}

impl Default for V4OrdinalIdentityLimits {
    fn default() -> Self {
        Self {
            max_requested: 65_536,
            coalesce_gap_bytes: 4_096,
            max_coalesced_read_bytes: STREAM_BYTES,
            max_tombstone_cache_bytes: STREAM_BYTES,
            max_ordinal_cache_bytes: STREAM_BYTES,
            max_descriptor_metadata_bytes: 16 * STREAM_BYTES,
        }
    }
}

/// Physical artifact purpose; a descriptor cannot be reused across domains.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum V4OrdinalArtifactKind {
    /// UUID-sorted forward identity state (authenticated but not read here).
    ForwardIdentities,
    /// Packed UUIDs in contiguous node-id ordinal order.
    OrdinalUuids,
    /// Sorted deleted node IDs.
    NodeTombstones,
}

/// One immutable generation-bound artifact.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalArtifact {
    /// Plain child filename under the index directory.
    pub name: String,
    /// Domain binding.
    pub kind: V4OrdinalArtifactKind,
    /// Topology generation that published the artifact.
    pub generation: u64,
    /// Exact authenticated byte length.
    pub bytes: u64,
    /// Lowercase SHA-256 digest.
    pub sha256: String,
    /// Required corruption checksum of the exact artifact bytes.
    #[serde(with = "crate::corruption_checksum::wire_hex")]
    pub xxh64: u64,
}

/// A packed contiguous reverse-identity range.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalRange {
    /// First nonzero node surrogate represented by byte zero.
    pub first_node_id: u64,
    /// Number of consecutive UUID records.
    pub count: u64,
    /// Immutable packed UUID artifact.
    pub artifact: V4OrdinalArtifact,
    /// Canonical authenticated fixed-size read fences.
    pub blocks: Vec<V4OrdinalBlock>,
}

/// One authenticated block of packed ordinal UUIDs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalBlock {
    /// Byte offset in the ordinal artifact.
    pub offset: u64,
    /// Number of UUID records in this block.
    pub count: u64,
    /// Required corruption checksum of this exact block.
    #[serde(with = "crate::corruption_checksum::wire_hex")]
    pub xxh64: u64,
}

/// A newest-generation sparse deletion override.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalTombstones {
    /// Generation whose deletions this file records.
    pub generation: u64,
    /// Sorted packed `u64` artifact.
    pub artifact: V4OrdinalArtifact,
    /// Authenticated fixed-size search fences for bounded selected reads.
    pub blocks: Vec<V4OrdinalTombstoneBlock>,
}

/// One authenticated sorted tombstone block fence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalTombstoneBlock {
    /// Byte offset in the tombstone artifact.
    pub offset: u64,
    /// Number of packed IDs in this block.
    pub count: u64,
    /// First ID in the block.
    pub first: u64,
    /// Last ID in the block.
    pub last: u64,
    /// Required corruption checksum of this exact block.
    #[serde(with = "crate::corruption_checksum::wire_hex")]
    pub xxh64: u64,
}

/// Generation-pinned v4 authority descriptor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalIdentityManifest {
    /// Must equal [`ORDINAL_IDENTITY_V4`].
    pub format_version: u32,
    /// Exact topology generation served by this snapshot.
    pub(crate) topology_generation: u64,
    /// Individually UUID-sorted immutable forward runs in strictly increasing
    /// publication-generation order, included to reject mixed authority.
    pub forward_identities: Vec<V4OrdinalArtifact>,
    /// Nonoverlapping packed ordinal ranges.
    pub ordinal_ranges: Vec<V4OrdinalRange>,
    /// Sparse deletion overrides in strictly increasing generation order.
    pub tombstones: Vec<V4OrdinalTombstones>,
    /// Whether UUIDs increase strictly across every ordinal, tombstoned or not,
    /// computed by the publisher from the records it streamed. Absent means
    /// unknown (every manifest written before the field existed), and readers
    /// then prove it by reading the ordinals; a publisher that cannot derive it
    /// omits it. Sits inside the SHA-256-authenticated manifest. It lets the
    /// ordered fast path decide in O(1) whether destinations can be walked in
    /// ordinal order, so a bounded query never pays for a scan of the graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid_order_matches_ordinals: Option<bool>,
}

/// Generation authority pinned by the caller's authenticated project receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V4OrdinalIdentityAuthority {
    /// Exact topology generation authorized by the project root.
    pub topology_generation: u64,
    /// Lowercase SHA-256 of the authorized membership manifest bytes.
    pub(crate) manifest_sha256: String,
}

/// Opaque ordinal authority authenticated through one pinned project generation.
#[derive(Clone, Debug)]
pub struct AuthenticatedV4OrdinalIdentityAuthority {
    pub(crate) authority: V4OrdinalIdentityAuthority,
}

impl AuthenticatedV4OrdinalIdentityAuthority {
    #[cfg(test)]
    pub(crate) fn authority(&self) -> &V4OrdinalIdentityAuthority {
        &self.authority
    }

    /// Open the selected ordinal facet at an admitted graph root.
    pub fn open(
        &self,
        graph_root: &Path,
        limits: V4OrdinalIdentityLimits,
    ) -> Result<V4OrdinalIdentityOpen, V4OrdinalIdentityError> {
        V4OrdinalIdentityHandle::open(graph_root, &self.authority, limits)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedOrdinalReceipt {
    nonce: String,
    expected_generation: u64,
    topology_delta_sha256: String,
    manifest_sha256: String,
}

impl crate::ResolvedProjectGeneration {
    /// Resolve the ordinal receipt through the pinned generation's authenticated
    /// graph/files participant. Absence is a clean rebuild requirement; any
    /// partial or malformed residue fails closed.
    pub fn authenticated_v4_ordinal_authority(
        &self,
    ) -> Result<Option<AuthenticatedV4OrdinalIdentityAuthority>, graphforge_core::GfError> {
        self.authenticated_v4_ordinal_manifest(&mut crate::GraphObjectIoTotals::default())
            .map(|value| value.map(|(authority, _)| authority))
    }

    pub(crate) fn authenticated_v4_ordinal_manifest(
        &self,
        io: &mut crate::GraphObjectIoTotals,
    ) -> Result<
        Option<(
            AuthenticatedV4OrdinalIdentityAuthority,
            V4OrdinalIdentityManifest,
        )>,
        graphforge_core::GfError,
    > {
        let mut targeted_state = crate::graph_manifest::GraphManifestTargetedState::default();
        let receipt = self.authenticated_graph_file_bytes_counted(
            RECEIPT_NAME,
            MAX_MANIFEST_BYTES,
            Some(&mut targeted_state),
            io,
        )?;
        let manifest = self.authenticated_graph_file_bytes_counted(
            ORDINAL_IDENTITY_MANIFEST,
            MAX_MANIFEST_BYTES,
            Some(&mut targeted_state),
            io,
        )?;
        match (receipt, manifest) {
            (None, None) => Ok(None),
            (None, Some(_)) | (Some(_), None) => Err(graphforge_core::GfError::Validation(
                "selected ordinal facet has incomplete authority residue".into(),
            )),
            (Some((_, receipt_bytes)), Some((manifest_entry, manifest_bytes))) => {
                let receipt: SelectedOrdinalReceipt = serde_json::from_slice(&receipt_bytes)
                    .map_err(|_| {
                        graphforge_core::GfError::Validation(
                            "selected ordinal receipt is malformed".into(),
                        )
                    })?;
                let generation = self
                    .authenticated_graph_file_bytes_counted(
                        GENERATION_NAME,
                        MAX_MANIFEST_BYTES,
                        Some(&mut targeted_state),
                        io,
                    )?
                    .ok_or_else(|| {
                        graphforge_core::GfError::Validation(
                            "selected topology generation authority is absent".into(),
                        )
                    })?;
                let generation: serde_json::Value =
                    serde_json::from_slice(&generation.1).map_err(|_| {
                        graphforge_core::GfError::Validation(
                            "selected topology generation authority is malformed".into(),
                        )
                    })?;
                let selected_generation = generation
                    .get("topology_generation")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        graphforge_core::GfError::Validation(
                            "selected topology generation is missing".into(),
                        )
                    })?;
                let manifest_digest = hex(&Sha256::digest(&manifest_bytes));
                let canonical_hex = |value: &str, length: usize| {
                    value.len() == length
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                };
                if !canonical_hex(&receipt.nonce, 32)
                    || !canonical_hex(&receipt.topology_delta_sha256, 64)
                    || receipt.expected_generation != selected_generation
                    || receipt.manifest_sha256 != manifest_entry.content_sha256
                    || receipt.manifest_sha256 != manifest_digest
                {
                    return Err(graphforge_core::GfError::Validation(
                        "selected ordinal receipt does not authenticate its manifest".into(),
                    ));
                }
                let parsed = parse_manifest(&manifest_bytes, selected_generation)
                    .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))?
                    .ok_or_else(|| {
                        graphforge_core::GfError::Validation(
                            "selected v4 manifest is legacy".into(),
                        )
                    })?;
                validate_manifest(&parsed, selected_generation)
                    .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))?;
                initial_admission_metrics(
                    &parsed,
                    manifest_bytes.len(),
                    manifest_bytes.capacity(),
                    V4OrdinalIdentityLimits::default(),
                )
                .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))?;
                Ok(Some((
                    AuthenticatedV4OrdinalIdentityAuthority {
                        authority: V4OrdinalIdentityAuthority {
                            topology_generation: selected_generation,
                            manifest_sha256: manifest_digest,
                        },
                    },
                    parsed,
                )))
            }
        }
    }
}

/// Typed open disposition. V3 is never parsed as v4.
#[derive(Debug)]
pub enum V4OrdinalIdentityOpen {
    /// Opened v4 handle. Descriptors are authenticated; artifact content is
    /// authenticated block by block as lookups read it.
    Ready(Box<V4OrdinalIdentityHandle>),
    /// A valid version marker that requires an explicit rebuild.
    RebuildRequired {
        /// Version found in the manifest.
        found_version: u32,
    },
}

/// Discovery result for the additive ordinal facet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum V4OrdinalIdentityDiscovery {
    /// The v4 path exists and must be authenticated by [`V4OrdinalIdentityHandle::open`].
    Present,
    /// Current canonical v3 node/edge authority is valid but ordinal v4 is absent.
    RebuildRequired {
        /// Canonical legacy facet version that requires ordinal construction.
        found_version: u32,
    },
}

/// Fail-closed v4 admission or lookup error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum V4OrdinalIdentityError {
    /// Filesystem or JSON operation failed.
    #[error("v4 ordinal identity I/O failed")]
    Io,
    /// Descriptor or manifest is noncanonical.
    #[error("v4 ordinal identity descriptor is invalid: {0}")]
    InvalidDescriptor(&'static str),
    /// Expected and manifest topology generations differ.
    #[error("v4 ordinal identity generation mismatch: expected {expected}, found {found}")]
    GenerationMismatch {
        /// Requested topology generation.
        expected: u64,
        /// Manifest topology generation.
        found: u64,
    },
    /// Immutable artifact authentication failed.
    #[error("v4 ordinal identity artifact authentication failed")]
    Authentication,
    /// Lookup was rejected before request-sized allocation.
    #[error("v4 ordinal identity request exceeds bound {maximum}: {requested}")]
    RequestLimit {
        /// Caller entry count including duplicates.
        requested: usize,
        /// Configured maximum.
        maximum: usize,
    },
}

/// A refusal reports the same public class as every other first-touch refusal
/// of committed data (`graph_admission`): corrupted or contradicting bytes are
/// a validation failure, not an execution one. Only a filesystem fault is a
/// storage error.
impl From<V4OrdinalIdentityError> for graphforge_core::GfError {
    fn from(error: V4OrdinalIdentityError) -> Self {
        match error {
            V4OrdinalIdentityError::Io => Self::Storage(error.to_string()),
            _ => Self::Validation(error.to_string()),
        }
    }
}

/// Sanitized failure classification for admission and lookup evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum V4OrdinalFailureKind {
    /// Filesystem or decoding failure.
    Io,
    /// Noncanonical or internally inconsistent descriptor state.
    InvalidDescriptor,
    /// The selected and encoded topology generations differ.
    GenerationMismatch,
    /// Authenticated bytes, identities, or retained capabilities disagree.
    Authentication,
    /// The caller batch exceeded its configured bound.
    RequestLimit,
}

/// Aggregate-only evidence for a failed admission or lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V4OrdinalFailureEvidence {
    /// Typed first failure without identities, paths, or record contents.
    pub kind: V4OrdinalFailureKind,
    /// One when authentication failed, otherwise zero.
    pub authentication_failures: u64,
}

impl V4OrdinalIdentityError {
    /// Return sanitized aggregate evidence for this first failure.
    #[must_use]
    pub const fn evidence(&self) -> V4OrdinalFailureEvidence {
        let kind = match self {
            Self::Io => V4OrdinalFailureKind::Io,
            Self::InvalidDescriptor(_) => V4OrdinalFailureKind::InvalidDescriptor,
            Self::GenerationMismatch { .. } => V4OrdinalFailureKind::GenerationMismatch,
            Self::Authentication => V4OrdinalFailureKind::Authentication,
            Self::RequestLimit { .. } => V4OrdinalFailureKind::RequestLimit,
        };
        V4OrdinalFailureEvidence {
            kind,
            authentication_failures: if matches!(self, Self::Authentication) {
                1
            } else {
                0
            },
        }
    }
}

/// Aggregate-only admission work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V4OrdinalAdmissionMetrics {
    /// Immutable files authenticated.
    pub artifacts: u64,
    /// Bytes read for authentication and bounded cross-run validation.
    pub authenticated_bytes: u64,
    /// Successful sequential artifact reads during admission.
    pub sequential_read_calls: u64,
    /// Largest single bounded admission buffer.
    pub peak_buffer_bytes: u64,
    /// Serialized manifest bytes retained transiently during admission.
    pub manifest_bytes: u64,
    /// Conservative metadata retained by the admitted handle.
    pub retained_descriptor_bytes: u64,
    /// Ordinal ranges admitted.
    pub ranges: u64,
    /// Tombstone runs admitted.
    pub tombstone_runs: u64,
}

/// Aggregate-only bounded lookup work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V4OrdinalLookupMetrics {
    /// Caller entries including duplicates.
    pub requested: u64,
    /// Unique requested node IDs.
    pub unique_requested: u64,
    /// Unique live identities found.
    pub found: u64,
    /// Range descriptors intersected.
    pub ranges_selected: u64,
    /// Coalesced ordinal payload calls.
    pub sequential_read_calls: u64,
    /// Ordinal and tombstone bytes read.
    pub bytes_read: u64,
    /// Requested identities hidden by newest tombstones.
    pub tombstoned: u64,
    /// Maximum anonymous request/index buffer charged by this lookup.
    pub peak_buffer_bytes: u64,
    /// Maximum retained tombstone-cache charge, including container metadata.
    pub retained_cache_bytes: u64,
    /// Maximum transient tombstone decode/clone charge.
    pub transient_buffer_bytes: u64,
    /// Per-identity seeks are forbidden by contract.
    pub per_record_seeks: u64,
    /// Generation-authentication file checks charged once to the pinned session.
    pub revalidation_calls: u64,
    /// Payload bytes read while revalidating the pinned session. Stamp and
    /// identity checks read metadata only, so this is normally zero.
    pub revalidation_bytes: u64,
}

/// Aggregate-only work required to pin an already admitted v4 authority to one
/// execution session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V4OrdinalRevalidationMetrics {
    /// Logical root/file identity and stamp checks performed.
    pub calls: u64,
    /// Artifact payload bytes read by those checks.
    pub bytes_read: u64,
}

/// One caller-ordered lookup result and its sanitized evidence.
#[derive(Debug, PartialEq, Eq)]
pub struct V4OrdinalLookup {
    /// UUID for each caller ID, or `None` when missing/deleted.
    pub values: Vec<Option<Uuid>>,
    /// Aggregate work evidence.
    pub metrics: V4OrdinalLookupMetrics,
}

#[derive(Debug)]
struct OpenArtifact {
    file: File,
    stamp: ArtifactStamp,
    descriptor: V4OrdinalArtifact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArtifactStamp {
    identity: FileIdentity,
    length: u64,
    modified: SystemTime,
}

#[derive(Debug)]
struct OpenRange {
    descriptor: V4OrdinalRange,
    artifact: OpenArtifact,
    /// A caller is relying on the manifest's claim that UUIDs ascend with
    /// ordinals, so every block this range serves must still ascend.
    verify_order: bool,
}

#[derive(Debug)]
struct OpenTombstones {
    artifact: OpenArtifact,
    blocks: Vec<TombstoneBlock>,
}

/// One immutable artifact retained by an authenticated v4 handle for a
/// generation-advancing writer. The cloned file pins the exact admitted inode;
/// callers must not reopen `descriptor.name` to obtain update input.
#[derive(Debug)]
pub(crate) struct PinnedV4OrdinalArtifact {
    pub(crate) descriptor: V4OrdinalArtifact,
    pub(crate) file: File,
}

/// The order of a manifest extended by a delta whose ordinals all follow the
/// parent's. Exact when both sides are known: a recorded inversion on either
/// side persists, and the boundary between the parent's last UUID and the
/// delta's first decides the rest. Unknown stays unknown.
pub(crate) fn combine_uuid_order(
    prior: Option<bool>,
    prior_last: Option<[u8; UUID_WIDTH_USIZE]>,
    delta: Option<bool>,
    delta_first: Option<[u8; UUID_WIDTH_USIZE]>,
) -> Option<bool> {
    match (prior, delta) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(match (prior_last, delta_first) {
            (Some(last), Some(first)) => last < first,
            _ => true,
        }),
        _ => None,
    }
}

impl V4OrdinalPinnedUpdateInputs {
    /// The UUID of the highest ordinal retained, read from the pinned,
    /// already-authenticated final range. `None` for an empty ordinal set.
    pub(crate) fn last_ordinal_uuid(
        &self,
    ) -> Result<Option<[u8; UUID_WIDTH_USIZE]>, graphforge_core::GfError> {
        let Some(last) = self
            .manifest
            .ordinal_ranges
            .iter()
            .max_by_key(|range| range.first_node_id)
        else {
            return Ok(None);
        };
        let pinned = self
            .artifacts
            .iter()
            .find(|artifact| artifact.descriptor.name == last.artifact.name)
            .ok_or_else(|| {
                graphforge_core::GfError::Storage("authenticated v4 update input is absent".into())
            })?;
        let storage = |error: std::io::Error| graphforge_core::GfError::Storage(error.to_string());
        let mut file = pinned.file.try_clone().map_err(storage)?;
        // The final record: UUID_WIDTH bytes back from the end.
        file.seek(SeekFrom::End(-16)).map_err(storage)?;
        let mut uuid = [0_u8; UUID_WIDTH_USIZE];
        file.read_exact(&mut uuid).map_err(storage)?;
        Ok(Some(uuid))
    }
}

/// Authenticated, inode-pinned inputs for planning the next v4 generation.
#[derive(Debug)]
pub(crate) struct V4OrdinalPinnedUpdateInputs {
    pub(crate) manifest: V4OrdinalIdentityManifest,
    pub(crate) artifacts: Vec<PinnedV4OrdinalArtifact>,
}

#[derive(Debug)]
struct TombstoneBlockCache {
    entries: BTreeMap<(usize, u64), Vec<u64>>,
    order: VecDeque<(usize, u64)>,
    charged_bytes: usize,
}

impl Default for TombstoneBlockCache {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            charged_bytes: TOMBSTONE_CACHE_FIXED_CHARGE,
        }
    }
}

impl TombstoneBlockCache {
    fn entry_charge(ids: &Vec<u64>) -> usize {
        TOMBSTONE_CACHE_ENTRY_CHARGE
            .saturating_add(ids.capacity().saturating_mul(std::mem::size_of::<u64>()))
    }
}

type TombstoneBlock = V4OrdinalTombstoneBlock;

/// An ordinal block whose bytes already passed their checksum.
#[derive(Debug)]
struct CachedOrdinalBlock {
    bytes: Vec<u8>,
    /// UUIDs ascend strictly inside this block, so a handle that later relies
    /// on the recorded order can still refuse it without rereading.
    ascends: bool,
}

/// Authenticated ordinal blocks, oldest evicted first, within a byte budget.
/// Blocks are immutable and were verified when read, so serving one again can
/// return nothing a fresh read would not.
#[derive(Debug, Default)]
struct OrdinalBlockCache {
    entries: BTreeMap<(usize, usize), CachedOrdinalBlock>,
    order: VecDeque<(usize, usize)>,
    charged_bytes: usize,
}

impl OrdinalBlockCache {
    fn insert(&mut self, key: (usize, usize), block: CachedOrdinalBlock, maximum: usize) {
        let charge = block.bytes.len();
        if charge > maximum || self.entries.contains_key(&key) {
            return;
        }
        while self.charged_bytes.saturating_add(charge) > maximum {
            let Some(oldest) = self.order.pop_front() else {
                return;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.charged_bytes = self.charged_bytes.saturating_sub(evicted.bytes.len());
            }
        }
        self.charged_bytes = self.charged_bytes.saturating_add(charge);
        self.order.push_back(key);
        self.entries.insert(key, block);
    }
}

/// What one lookup may spend reading one ordinal range.
struct RangeRead<'a> {
    range_index: usize,
    cache: &'a mut OrdinalBlockCache,
    max_cache_bytes: usize,
    gap_bytes: u64,
    maximum_read_bytes: usize,
    retained_buffer_bytes: u64,
}

/// Authenticated generation-pinned read handle.
#[derive(Debug)]
pub struct V4OrdinalIdentityHandle {
    root: StableDirectory,
    coordination_file: File,
    coordination_identity: FileIdentity,
    manifest_file: File,
    manifest_stamp: ArtifactStamp,
    topology_generation: u64,
    forward: Vec<OpenArtifact>,
    ranges: Vec<OpenRange>,
    tombstones: Vec<OpenTombstones>,
    tombstone_cache: TombstoneBlockCache,
    ordinal_cache: OrdinalBlockCache,
    limits: V4OrdinalIdentityLimits,
    admission: V4OrdinalAdmissionMetrics,
    /// Whether every artifact byte has been authenticated and every
    /// cross-artifact invariant proven ([`Self::admit_complete`]). Opening
    /// reads no artifact bytes: ordinal and tombstone blocks authenticate on
    /// the lookup that reads them.
    complete: bool,
    /// Memoized proof, by reading the ordinals, that UUIDs increase strictly.
    uuid_order_matches_ordinals: Option<bool>,
    /// The publisher's claim, from the authenticated manifest.
    recorded_order: Option<bool>,
    /// The seams between ordinal ranges were checked against the record.
    seams_checked: bool,
}

impl V4OrdinalIdentityHandle {
    /// Revalidate this retained generation and clone its already-authenticated
    /// artifact handles for a bounded append/compaction planner.
    pub(crate) fn pinned_update_inputs(
        &mut self,
    ) -> Result<V4OrdinalPinnedUpdateInputs, V4OrdinalIdentityError> {
        // A writer builds the next generation from these bytes, so it never
        // inherits them unauthenticated: forward runs have no block fences and
        // no query reads them, which makes this their first authentication.
        self.admit_complete()?;
        self.revalidate()?;
        let manifest = V4OrdinalIdentityManifest {
            format_version: ORDINAL_IDENTITY_V4,
            topology_generation: self.topology_generation,
            forward_identities: self
                .forward
                .iter()
                .map(|artifact| artifact.descriptor.clone())
                .collect(),
            ordinal_ranges: self
                .ranges
                .iter()
                .map(|range| range.descriptor.clone())
                .collect(),
            tombstones: self
                .tombstones
                .iter()
                .map(|run| V4OrdinalTombstones {
                    generation: run.artifact.descriptor.generation,
                    artifact: run.artifact.descriptor.clone(),
                    blocks: run.blocks.clone(),
                })
                .collect(),
            // What complete admission derived from the authenticated data, not
            // the record: a legacy manifest that never recorded the fact is
            // promoted by the first generation built on it.
            uuid_order_matches_ordinals: self.uuid_order_matches_ordinals,
        };
        let artifacts = self
            .forward
            .iter()
            .chain(self.ranges.iter().map(|range| &range.artifact))
            .chain(self.tombstones.iter().map(|run| &run.artifact))
            .map(|artifact| {
                Ok(PinnedV4OrdinalArtifact {
                    descriptor: artifact.descriptor.clone(),
                    file: artifact.file.try_clone().map_err(io_error)?,
                })
            })
            .collect::<Result<Vec<_>, V4OrdinalIdentityError>>()?;
        Ok(V4OrdinalPinnedUpdateInputs {
            manifest,
            artifacts,
        })
    }

    /// Return the exact v4 facet names admitted through this retained,
    /// generation-authenticated handle.
    pub(crate) fn referenced_file_names(&self) -> BTreeSet<String> {
        std::iter::once(MANIFEST_NAME.to_owned())
            .chain(std::iter::once(RECEIPT_FILE_NAME.to_owned()))
            .chain(std::iter::once(LOCK_NAME.to_owned()))
            .chain(
                self.forward
                    .iter()
                    .map(|artifact| artifact.descriptor.name.clone()),
            )
            .chain(
                self.ranges
                    .iter()
                    .map(|range| range.artifact.descriptor.name.clone()),
            )
            .chain(
                self.tombstones
                    .iter()
                    .map(|run| run.artifact.descriptor.name.clone()),
            )
            .collect()
    }

    /// Classify the additive ordinal facet without treating a present file as
    /// trusted. A present malformed v4 remains `Present` and subsequently
    /// fails authenticated open; discovery never falls back around it.
    pub fn discover(
        project_dir: &Path,
        topology_generation: u64,
    ) -> Result<V4OrdinalIdentityDiscovery, V4OrdinalIdentityError> {
        let root = StableDirectory::open(&project_dir.join(INDEX_DIR)).map_err(io_error)?;
        match root.open_child_file(MANIFEST_NAME.as_ref()) {
            Ok(_) => return Ok(V4OrdinalIdentityDiscovery::Present),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        if crate::UuidMembershipIndex::open_at_generation(project_dir, topology_generation).is_err()
        {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "current v3 authority failed authentication",
            ));
        }
        Ok(V4OrdinalIdentityDiscovery::RebuildRequired { found_version: 3 })
    }

    /// Open one immutable v4 generation without reading artifact bytes.
    ///
    /// The manifest is authenticated against the pinned authority, every
    /// descriptor and block fence is validated, and every artifact is opened,
    /// checked for a single link and exact length, and pinned by inode. Content
    /// is authenticated when it is read: a lookup checks the required XXH64 of
    /// every ordinal and tombstone block it returns a value from, and refuses
    /// before returning any value derived from a block that fails. Open cost is
    /// therefore proportional to descriptors and files, not to nodes.
    ///
    /// Forward runs carry only a whole-artifact checksum and are read only by
    /// writers; the cross-artifact invariants (mapping commitments, UUID
    /// uniqueness, canonical order) are proven by [`Self::admit_complete`].
    pub(crate) fn open(
        project_dir: &Path,
        authority: &V4OrdinalIdentityAuthority,
        limits: V4OrdinalIdentityLimits,
    ) -> Result<V4OrdinalIdentityOpen, V4OrdinalIdentityError> {
        validate_limits(limits)?;
        let root = StableDirectory::open(&project_dir.join(INDEX_DIR)).map_err(io_error)?;
        let coordination_file = root.open_child_file(LOCK_NAME.as_ref()).map_err(io_error)?;
        if file_link_count(&coordination_file).map_err(io_error)? != 1 {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        <File as fs4::FileExt>::lock_shared(&coordination_file).map_err(io_error)?;
        let coordination_identity = file_identity(&coordination_file).map_err(io_error)?;
        let mut manifest_file = root
            .open_child_file(MANIFEST_NAME.as_ref())
            .map_err(io_error)?;
        let manifest_stamp = artifact_stamp(&manifest_file)?;
        if file_link_count(&manifest_file).map_err(io_error)? != 1 {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
        authenticate_manifest_authority(&body, authority)?;
        let manifest = parse_manifest(&body, authority.topology_generation)?.ok_or(
            V4OrdinalIdentityError::InvalidDescriptor("v3 occupies the v4 manifest path"),
        )?;
        validate_manifest(&manifest, authority.topology_generation)?;

        let mut admission =
            initial_admission_metrics(&manifest, body.len(), body.capacity(), limits)?;
        let mut names = BTreeSet::new();
        let mut forward = Vec::with_capacity(manifest.forward_identities.len());
        for artifact in &manifest.forward_identities {
            require_kind(
                artifact,
                V4OrdinalArtifactKind::ForwardIdentities,
                &manifest,
            )?;
            if !names.insert(artifact.name.clone()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "artifact filename is reused",
                ));
            }
            forward.push(open_admission_file(&root, artifact)?);
        }
        let mut ranges = Vec::with_capacity(manifest.ordinal_ranges.len());
        for range in &manifest.ordinal_ranges {
            if !names.insert(range.artifact.name.clone()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "artifact filename is reused",
                ));
            }
            ranges.push(OpenRange {
                descriptor: range.clone(),
                artifact: open_admission_file(&root, &range.artifact)?,
                verify_order: false,
            });
        }
        let mut tombstones = Vec::with_capacity(manifest.tombstones.len());
        for run in &manifest.tombstones {
            if !names.insert(run.artifact.name.clone()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "artifact filename is reused",
                ));
            }
            tombstones.push(OpenTombstones {
                artifact: open_admission_file(&root, &run.artifact)?,
                blocks: run.blocks.clone(),
            });
        }
        // The coordination lock protects admission from a concurrent writer,
        // not the lifetime of an immutable snapshot. Releasing it here keeps
        // retained handles from starving publication. Revalidation below
        // still pins the manifest, lock inode, and every immutable artifact.
        <File as fs4::FileExt>::unlock(&coordination_file).map_err(io_error)?;
        admission.ranges = ranges.len() as u64;
        admission.tombstone_runs = tombstones.len() as u64;
        Ok(V4OrdinalIdentityOpen::Ready(Box::new(Self {
            root,
            coordination_file,
            coordination_identity,
            manifest_file,
            manifest_stamp,
            topology_generation: authority.topology_generation,
            forward,
            ranges,
            tombstones,
            tombstone_cache: TombstoneBlockCache::default(),
            ordinal_cache: OrdinalBlockCache::default(),
            limits,
            admission,
            complete: false,
            uuid_order_matches_ordinals: None,
            recorded_order: manifest.uuid_order_matches_ordinals,
            seams_checked: false,
        })))
    }

    /// Authenticate every artifact byte and prove every cross-artifact
    /// invariant: block fences against content, nonzero canonical UUIDs,
    /// forward and ordinal mapping commitments equal, forward UUIDs unique
    /// across runs, tombstones sorted and known, and the UUID-order flag.
    /// Reads every artifact once (twice for forward runs) through the inodes
    /// pinned at open. Memoized: a complete handle does no further work.
    ///
    /// Writers call this before building on the artifacts
    /// ([`Self::pinned_update_inputs`]); lookups never need it.
    ///
    /// # Errors
    /// Returns the first authentication or descriptor refusal.
    pub(crate) fn admit_complete(&mut self) -> Result<(), V4OrdinalIdentityError> {
        if self.complete {
            return Ok(());
        }
        let mut forward_commitment = MappingCommitment::default();
        let mut ordinal_commitment = MappingCommitment::default();
        for artifact in &mut self.forward {
            admit_forward_artifact(
                artifact,
                &self.ranges,
                &mut forward_commitment,
                &mut self.admission,
            )?;
        }
        validate_unique_forward_runs(&mut self.forward, &mut self.admission)?;
        let mut prior_uuid = None;
        let mut uuid_order_matches_ordinals = true;
        for range in &mut self.ranges {
            admit_ordinal_artifact(
                &mut range.artifact,
                &range.descriptor,
                &mut ordinal_commitment,
                &mut self.admission,
                &mut prior_uuid,
                &mut uuid_order_matches_ordinals,
            )?;
        }
        if forward_commitment != ordinal_commitment {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward and ordinal identity authorities disagree",
            ));
        }
        for run in &mut self.tombstones {
            admit_tombstone_artifact(
                &mut run.artifact,
                &run.blocks,
                &self.ranges,
                &mut self.admission,
            )?;
        }
        if self
            .recorded_order
            .is_some_and(|recorded| recorded != uuid_order_matches_ordinals)
        {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "recorded ordinal UUID order disagrees with the ordinals",
            ));
        }
        self.uuid_order_matches_ordinals = Some(uuid_order_matches_ordinals);
        self.complete = true;
        Ok(())
    }

    /// Admission evidence for this retained handle.
    #[must_use]
    pub const fn admission_metrics(&self) -> V4OrdinalAdmissionMetrics {
        self.admission
    }

    /// Authenticated topology generation.
    #[must_use]
    pub const fn topology_generation(&self) -> u64 {
        self.topology_generation
    }

    /// Whether authenticated UUIDs increase strictly across all ordinal ranges.
    /// This conservative proof includes tombstoned identities: removing any of
    /// them preserves ordering.
    ///
    /// A manifest that records the fact answers in O(1) without reading an
    /// ordinal. A recorded `true` is a claim the publisher computed from the
    /// records it streamed, not something this call proved, so from then on
    /// every block a lookup reads must itself ascend; a block that does not is
    /// refused ([`Self::admit_complete`] checks the whole claim). A manifest
    /// that is silent (published before the field) is proven by reading and
    /// authenticating every ordinal block once, stopping at the first
    /// inversion, and memoized.
    ///
    /// # Errors
    /// Returns the refusal for a block that fails authentication; a corrupted
    /// block is never reported as merely "unordered".
    pub fn uuid_order_matches_ordinals(&mut self) -> Result<bool, V4OrdinalIdentityError> {
        if let Some(ordered) = self.uuid_order_matches_ordinals {
            return Ok(ordered);
        }
        match self.recorded_order {
            Some(false) => return Ok(false),
            Some(true) => {
                for range in &mut self.ranges {
                    range.verify_order = true;
                }
                // A block is checked inside itself, so the seams between ranges
                // are checked once here: O(ranges), never per node.
                self.check_range_seams()?;
                return Ok(true);
            }
            None => {}
        }
        let ordered = scan_uuid_order(&mut self.ranges, &mut self.admission)?;
        self.uuid_order_matches_ordinals = Some(ordered);
        Ok(ordered)
    }

    /// The last UUID of every range must sort below the first UUID of the next,
    /// read through the authenticated block path. Done once per handle.
    fn check_range_seams(&mut self) -> Result<(), V4OrdinalIdentityError> {
        if self.seams_checked {
            return Ok(());
        }
        let mut prior_last: Option<Uuid> = None;
        for range_index in 0..self.ranges.len() {
            let descriptor = &self.ranges[range_index].descriptor;
            let first_id = descriptor.first_node_id;
            let last_id = first_id + descriptor.count - 1;
            let ids = [first_id, last_id];
            let mut resolved = BTreeMap::new();
            read_range_coalesced(
                &mut self.ranges[range_index],
                &ids,
                RangeRead {
                    range_index,
                    cache: &mut self.ordinal_cache,
                    max_cache_bytes: self.limits.max_ordinal_cache_bytes,
                    gap_bytes: self.limits.coalesce_gap_bytes,
                    maximum_read_bytes: self.limits.max_coalesced_read_bytes,
                    retained_buffer_bytes: 0,
                },
                &mut resolved,
                &mut V4OrdinalLookupMetrics::default(),
            )?;
            let (first, last) = (resolved[&first_id], resolved[&last_id]);
            if prior_last.is_some_and(|prior| prior.as_bytes() >= first.as_bytes()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "ordinal UUIDs contradict the recorded UUID order",
                ));
            }
            prior_last = Some(last);
        }
        self.seams_checked = true;
        Ok(())
    }

    /// Maximum request entries accepted by this admitted handle per lookup.
    #[must_use]
    pub const fn max_requested_ids(&self) -> usize {
        self.limits.max_requested
    }

    /// Resolve a bounded caller batch while preserving caller order.
    pub fn lookup_node_uuids(
        &mut self,
        requested: &[u64],
    ) -> Result<V4OrdinalLookup, V4OrdinalIdentityError> {
        let revalidation = self.revalidate_for_session()?;
        let mut lookup = self.lookup_node_uuids_pinned(requested)?;
        lookup.metrics.revalidation_calls = revalidation.calls;
        lookup.metrics.revalidation_bytes = revalidation.bytes_read;
        Ok(lookup)
    }

    /// Authenticate the retained authority once before sharing it with one
    /// execution session. Subsequent lookups through that session use the
    /// already-open immutable handles and must not repeat the artifact walk.
    #[doc(hidden)]
    pub fn revalidate_for_session(
        &self,
    ) -> Result<V4OrdinalRevalidationMetrics, V4OrdinalIdentityError> {
        self.revalidate()?;
        let artifacts = self
            .forward
            .len()
            .saturating_add(self.ranges.len())
            .saturating_add(self.tombstones.len());
        Ok(V4OrdinalRevalidationMetrics {
            // Root identity, retained+named coordination, retained+named
            // manifest, then retained+named checks for every artifact.
            calls: 5_u64.saturating_add(
                u64::try_from(artifacts)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2),
            ),
            bytes_read: 0,
        })
    }

    /// Resolve through an authority already authenticated for the surrounding
    /// execution session.
    #[doc(hidden)]
    pub fn lookup_node_uuids_pinned(
        &mut self,
        requested: &[u64],
    ) -> Result<V4OrdinalLookup, V4OrdinalIdentityError> {
        if requested.len() > self.limits.max_requested {
            return Err(V4OrdinalIdentityError::RequestLimit {
                requested: requested.len(),
                maximum: self.limits.max_requested,
            });
        }
        let mut metrics = V4OrdinalLookupMetrics {
            requested: requested.len() as u64,
            // The request, plus the ordinal blocks the handle already holds.
            peak_buffer_bytes: (requested.len() as u64)
                .saturating_mul(REQUEST_ENTRY_CHARGE)
                .saturating_add(self.ordinal_cache.charged_bytes as u64),
            retained_cache_bytes: (self.tombstone_cache.charged_bytes
                + self.ordinal_cache.charged_bytes) as u64,
            ..Default::default()
        };
        let request_buffer_bytes = metrics.peak_buffer_bytes;
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let deleted = self.lookup_tombstones(&unique, request_buffer_bytes, &mut metrics)?;
        let live = unique
            .iter()
            .copied()
            .filter(|id| !deleted.contains(id))
            .collect::<Vec<_>>();
        let mut resolved = BTreeMap::new();
        for (range_index, range) in self.ranges.iter_mut().enumerate() {
            let first = range.descriptor.first_node_id;
            let last = first + range.descriptor.count - 1;
            let start = live.partition_point(|id| *id < first);
            let end = live.partition_point(|id| *id <= last);
            if start == end {
                continue;
            }
            metrics.ranges_selected = metrics.ranges_selected.saturating_add(1);
            read_range_coalesced(
                range,
                &live[start..end],
                RangeRead {
                    range_index,
                    cache: &mut self.ordinal_cache,
                    max_cache_bytes: self.limits.max_ordinal_cache_bytes,
                    gap_bytes: self.limits.coalesce_gap_bytes,
                    maximum_read_bytes: self.limits.max_coalesced_read_bytes,
                    retained_buffer_bytes: request_buffer_bytes
                        .saturating_add(self.tombstone_cache.charged_bytes as u64),
                },
                &mut resolved,
                &mut metrics,
            )?;
        }
        metrics.retained_cache_bytes = metrics
            .retained_cache_bytes
            .max((self.tombstone_cache.charged_bytes + self.ordinal_cache.charged_bytes) as u64);
        metrics.found = resolved.len() as u64;
        metrics.tombstoned = deleted.len() as u64;
        Ok(V4OrdinalLookup {
            values: requested
                .iter()
                .map(|id| resolved.get(id).copied())
                .collect(),
            metrics,
        })
    }

    fn lookup_tombstones(
        &mut self,
        requested: &BTreeSet<u64>,
        request_buffer_bytes: u64,
        metrics: &mut V4OrdinalLookupMetrics,
    ) -> Result<BTreeSet<u64>, V4OrdinalIdentityError> {
        let mut deleted = BTreeSet::new();
        for (run_index, run) in self.tombstones.iter_mut().enumerate() {
            for block_index in 0..run.blocks.len() {
                let block = run.blocks[block_index].clone();
                if requested.range(block.first..=block.last).next().is_none() {
                    continue;
                }
                let ids = read_tombstone_block(
                    run,
                    run_index,
                    block_index,
                    self.limits.max_tombstone_cache_bytes,
                    request_buffer_bytes,
                    &mut self.tombstone_cache,
                    metrics,
                )?;
                for id in ids {
                    if requested.contains(&id) {
                        deleted.insert(id);
                    }
                }
            }
        }
        Ok(deleted)
    }

    fn revalidate(&self) -> Result<(), V4OrdinalIdentityError> {
        self.root.revalidate_named().map_err(io_error)?;
        let named_coordination = self
            .root
            .open_child_file(LOCK_NAME.as_ref())
            .map_err(io_error)?;
        if file_identity(&self.coordination_file).map_err(io_error)? != self.coordination_identity
            || file_identity(&named_coordination).map_err(io_error)? != self.coordination_identity
        {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        if artifact_stamp(&self.manifest_file)? != self.manifest_stamp {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let named_manifest = self
            .root
            .open_child_file(MANIFEST_NAME.as_ref())
            .map_err(io_error)?;
        if artifact_stamp(&named_manifest)? != self.manifest_stamp {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        for artifact in self
            .forward
            .iter()
            .chain(self.ranges.iter().map(|range| &range.artifact))
            .chain(self.tombstones.iter().map(|run| &run.artifact))
        {
            if artifact_stamp(&artifact.file)? != artifact.stamp {
                return Err(V4OrdinalIdentityError::Authentication);
            }
            let named = self
                .root
                .open_child_file(artifact.descriptor.name.as_ref())
                .map_err(io_error)?;
            if artifact_stamp(&named)? != artifact.stamp {
                return Err(V4OrdinalIdentityError::Authentication);
            }
        }
        Ok(())
    }
}

pub(crate) fn decode_construction_ordinal_manifest(
    bytes: &[u8],
    generation: u64,
) -> Result<V4OrdinalIdentityManifest, V4OrdinalIdentityError> {
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "manifest exceeds admission bound",
        ));
    }
    let manifest = parse_manifest(bytes, generation)?.ok_or(
        V4OrdinalIdentityError::InvalidDescriptor("construction ordinal manifest is legacy"),
    )?;
    validate_manifest(&manifest, generation)?;
    initial_admission_metrics(
        &manifest,
        bytes.len(),
        bytes.len(),
        V4OrdinalIdentityLimits::default(),
    )?;
    Ok(manifest)
}

fn authenticate_manifest_authority(
    body: &[u8],
    authority: &V4OrdinalIdentityAuthority,
) -> Result<(), V4OrdinalIdentityError> {
    if authority.manifest_sha256.len() != 64
        || !authority
            .manifest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || hex(&Sha256::digest(body)) != authority.manifest_sha256
    {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    Ok(())
}

fn initial_admission_metrics(
    manifest: &V4OrdinalIdentityManifest,
    manifest_bytes: usize,
    manifest_capacity: usize,
    limits: V4OrdinalIdentityLimits,
) -> Result<V4OrdinalAdmissionMetrics, V4OrdinalIdentityError> {
    let retained_descriptor_bytes = descriptor_metadata_charge(manifest);
    if retained_descriptor_bytes > limits.max_descriptor_metadata_bytes as u64 {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "descriptor metadata exceeds admission bound",
        ));
    }
    Ok(V4OrdinalAdmissionMetrics {
        manifest_bytes: manifest_bytes as u64,
        retained_descriptor_bytes,
        peak_buffer_bytes: retained_descriptor_bytes
            .saturating_add(manifest_capacity as u64)
            .saturating_add(ADMISSION_TRANSIENT_FIXED_CHARGE),
        ..Default::default()
    })
}

fn validate_limits(limits: V4OrdinalIdentityLimits) -> Result<(), V4OrdinalIdentityError> {
    if limits.max_requested == 0
        || limits.max_coalesced_read_bytes < ORDINAL_BLOCK_BYTES_USIZE
        || limits.max_tombstone_cache_bytes < TOMBSTONE_CACHE_FIXED_CHARGE
        || limits.max_descriptor_metadata_bytes < DESCRIPTOR_FIXED_CHARGE
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "lookup bounds are invalid",
        ));
    }
    Ok(())
}

fn descriptor_metadata_charge(manifest: &V4OrdinalIdentityManifest) -> u64 {
    let artifact = |descriptor: &V4OrdinalArtifact| {
        ARTIFACT_DESCRIPTOR_CHARGE
            .saturating_add(descriptor.name.len() as u64)
            .saturating_add(descriptor.sha256.len() as u64)
    };
    let forward = manifest
        .forward_identities
        .iter()
        .fold(0_u64, |sum, item| sum.saturating_add(artifact(item)));
    let ordinal = manifest.ordinal_ranges.iter().fold(0_u64, |sum, range| {
        sum.saturating_add(artifact(&range.artifact))
            .saturating_add(
                (range.blocks.len() as u64).saturating_mul(ORDINAL_BLOCK_DESCRIPTOR_CHARGE),
            )
    });
    let tombstones = manifest.tombstones.iter().fold(0_u64, |sum, run| {
        sum.saturating_add(artifact(&run.artifact)).saturating_add(
            (run.blocks.len() as u64).saturating_mul(TOMBSTONE_BLOCK_DESCRIPTOR_CHARGE),
        )
    });
    (DESCRIPTOR_FIXED_CHARGE as u64)
        .saturating_add(forward)
        .saturating_add(ordinal)
        .saturating_add(tombstones)
}

fn parse_manifest(
    body: &[u8],
    expected_generation: u64,
) -> Result<Option<V4OrdinalIdentityManifest>, V4OrdinalIdentityError> {
    let _ = expected_generation;
    decode_ordinal_manifest(body).map(Some)
}

pub(crate) fn decode_ordinal_manifest(
    body: &[u8],
) -> Result<V4OrdinalIdentityManifest, V4OrdinalIdentityError> {
    if body.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal manifest exceeds bound",
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(body).map_err(io_error)?;
    if value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        != Some(u64::from(ORDINAL_IDENTITY_V4))
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "format version is unsupported; recreate the ordinal index",
        ));
    }
    serde_json::from_value(value).map_err(io_error)
}

fn validate_manifest(
    manifest: &V4OrdinalIdentityManifest,
    expected_generation: u64,
) -> Result<(), V4OrdinalIdentityError> {
    if manifest.topology_generation != expected_generation {
        return Err(V4OrdinalIdentityError::GenerationMismatch {
            expected: expected_generation,
            found: manifest.topology_generation,
        });
    }
    let mut names = BTreeSet::new();
    for artifact in manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
    {
        if !names.insert(artifact.name.as_str()) {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "artifact filename is reused",
            ));
        }
    }
    if manifest.forward_identities.is_empty() {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "forward identity authority is absent",
        ));
    }
    if manifest.forward_identities.len() > STREAM_BYTES / FORWARD_RECORD_WIDTH_USIZE {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "forward run count exceeds bounded admission cursors",
        ));
    }
    let mut prior_forward_generation = 0;
    for run in &manifest.forward_identities {
        require_kind(run, V4OrdinalArtifactKind::ForwardIdentities, manifest)?;
        if run.generation <= prior_forward_generation {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward runs are not in canonical generation order",
            ));
        }
        prior_forward_generation = run.generation;
    }
    let mut prior_end = 0_u64;
    for range in &manifest.ordinal_ranges {
        require_kind(
            &range.artifact,
            V4OrdinalArtifactKind::OrdinalUuids,
            manifest,
        )?;
        let end = range
            .first_node_id
            .checked_add(range.count.checked_sub(1).ok_or(
                V4OrdinalIdentityError::InvalidDescriptor("ordinal range is empty"),
            )?)
            .ok_or(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal range overflows",
            ))?;
        if range.first_node_id == 0 || range.first_node_id <= prior_end {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal ranges overlap or descend",
            ));
        }
        let bytes = range.count.checked_mul(UUID_WIDTH).ok_or(
            V4OrdinalIdentityError::InvalidDescriptor("ordinal byte length overflows"),
        )?;
        if range.artifact.bytes != bytes {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal artifact length is not packed",
            ));
        }
        validate_ordinal_block_fences(range)?;
        prior_end = end;
    }
    validate_tombstone_descriptors(manifest)
}

/// Descriptor-only proof that an ordinal range's blocks tile its artifact in
/// canonical `ORDINAL_BLOCK_BYTES` strides. Content is checked against each
/// block's checksum when the block is read, so open needs no byte of it.
fn validate_ordinal_block_fences(range: &V4OrdinalRange) -> Result<(), V4OrdinalIdentityError> {
    let noncanonical =
        || V4OrdinalIdentityError::InvalidDescriptor("ordinal block fences are noncanonical");
    let records_per_block = ORDINAL_BLOCK_BYTES / UUID_WIDTH;
    let expected = range.count.div_ceil(records_per_block);
    if u64::try_from(range.blocks.len()).map_err(|_| noncanonical())? != expected {
        return Err(noncanonical());
    }
    let mut remaining = range.count;
    for (index, block) in range.blocks.iter().enumerate() {
        let count = remaining.min(records_per_block);
        if block.offset != u64::try_from(index).map_err(|_| noncanonical())? * ORDINAL_BLOCK_BYTES
            || block.count != count
        {
            return Err(noncanonical());
        }
        remaining -= count;
    }
    Ok(())
}

fn validate_tombstone_descriptors(
    manifest: &V4OrdinalIdentityManifest,
) -> Result<(), V4OrdinalIdentityError> {
    let mut prior_generation = 0;
    for run in &manifest.tombstones {
        require_kind(
            &run.artifact,
            V4OrdinalArtifactKind::NodeTombstones,
            manifest,
        )?;
        if run.generation == 0
            || run.generation <= prior_generation
            || run.generation != run.artifact.generation
            || run.artifact.bytes % TOMBSTONE_WIDTH != 0
        {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "tombstone runs are noncanonical",
            ));
        }
        prior_generation = run.generation;
        validate_tombstone_block_fences(run)?;
    }
    Ok(())
}

/// Descriptor-only proof that a tombstone run's blocks tile its artifact in
/// canonical strides with ascending, non-overlapping ID fences. The IDs inside
/// a block are checked against its fence when that block is read.
fn validate_tombstone_block_fences(
    run: &V4OrdinalTombstones,
) -> Result<(), V4OrdinalIdentityError> {
    let noncanonical =
        || V4OrdinalIdentityError::InvalidDescriptor("tombstone block fences are noncanonical");
    let records_per_block = TOMBSTONE_BLOCK_BYTES / TOMBSTONE_WIDTH;
    let total = run.artifact.bytes / TOMBSTONE_WIDTH;
    if u64::try_from(run.blocks.len()).map_err(|_| noncanonical())?
        != total.div_ceil(records_per_block)
    {
        return Err(noncanonical());
    }
    let mut remaining = total;
    let mut prior_last = 0_u64;
    for (index, block) in run.blocks.iter().enumerate() {
        let count = remaining.min(records_per_block);
        if block.offset != u64::try_from(index).map_err(|_| noncanonical())? * TOMBSTONE_BLOCK_BYTES
            || block.count != count
            || block.first == 0
            || block.first > block.last
            || block.first <= prior_last
        {
            return Err(noncanonical());
        }
        prior_last = block.last;
        remaining -= count;
    }
    Ok(())
}

fn require_kind(
    artifact: &V4OrdinalArtifact,
    kind: V4OrdinalArtifactKind,
    manifest: &V4OrdinalIdentityManifest,
) -> Result<(), V4OrdinalIdentityError> {
    if artifact.kind != kind
        || artifact.generation == 0
        || artifact.generation > manifest.topology_generation
        || artifact.sha256.len() != 64
        || !artifact
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !plain_name(&artifact.name)
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "artifact binding is noncanonical",
        ));
    }
    if kind == V4OrdinalArtifactKind::ForwardIdentities
        && !artifact.bytes.is_multiple_of(FORWARD_RECORD_WIDTH)
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "forward identity artifact is truncated",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MappingCommitment {
    count: u64,
    limbs: [u64; 4],
}

impl MappingCommitment {
    fn add(&mut self, uuid: &[u8; UUID_WIDTH_USIZE], node_id: u64) {
        let mut digest = graphforge_core::hash_observation::ContractSha256::new();
        digest.update(b"graphforge-v4-ordinal-mapping\0");
        digest.update(uuid);
        digest.update(node_id.to_be_bytes());
        let digest = digest.finalize();
        for (limb, bytes) in self.limbs.iter_mut().zip(digest.chunks_exact(8)) {
            *limb = limb.wrapping_add(u64::from_be_bytes(bytes.try_into().expect("SHA limb")));
        }
        self.count = self.count.saturating_add(1);
    }
}

fn admit_ordinal_artifact(
    artifact: &mut OpenArtifact,
    range: &V4OrdinalRange,
    commitment: &mut MappingCommitment,
    metrics: &mut V4OrdinalAdmissionMetrics,
    prior_uuid: &mut Option<[u8; UUID_WIDTH_USIZE]>,
    uuid_order_matches_ordinals: &mut bool,
) -> Result<(), V4OrdinalIdentityError> {
    artifact.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let block_bytes =
        usize::try_from(ORDINAL_BLOCK_BYTES.min(artifact.descriptor.bytes.max(UUID_WIDTH)))
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
    let mut buffer = vec![0_u8; block_bytes];
    let mut whole = crate::corruption_checksum::Checksum::new();
    let mut declared = range.blocks.iter();
    let mut offset = 0_u64;
    let mut ordinal = 0_u64;
    loop {
        let read = read_fill_or_eof(&mut artifact.file, &mut buffer, metrics)?;
        if read == 0 {
            break;
        }
        whole.update(&buffer[..read]);
        if !read.is_multiple_of(UUID_WIDTH_USIZE) {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        if !declared.next().is_some_and(|block| {
            block.offset == offset
                && block.count == read as u64 / UUID_WIDTH
                && block.xxh64 == crate::corruption_checksum::checksum(&buffer[..read])
        }) {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal block fences are noncanonical",
            ));
        }
        for record in buffer[..read].chunks_exact(UUID_WIDTH_USIZE) {
            let uuid: [u8; UUID_WIDTH_USIZE] = record.try_into().expect("fixed UUID");
            if uuid == [0; UUID_WIDTH_USIZE] {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "ordinal UUID is zero",
                ));
            }
            let node_id = range
                .first_node_id
                .checked_add(ordinal)
                .ok_or(V4OrdinalIdentityError::Authentication)?;
            if prior_uuid.is_some_and(|prior| prior >= uuid) {
                *uuid_order_matches_ordinals = false;
            }
            *prior_uuid = Some(uuid);
            commitment.add(&uuid, node_id);
            ordinal = ordinal.saturating_add(1);
        }
        offset = offset.saturating_add(read as u64);
    }
    if declared.next().is_some() {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal block fences are noncanonical",
        ));
    }
    finish_admission(artifact, &whole, metrics)
}

fn admit_forward_artifact(
    artifact: &mut OpenArtifact,
    ranges: &[OpenRange],
    commitment: &mut MappingCommitment,
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    artifact.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let maximum_stream = (STREAM_BYTES / FORWARD_RECORD_WIDTH_USIZE) * FORWARD_RECORD_WIDTH_USIZE;
    let artifact_bytes = usize::try_from(artifact.descriptor.bytes).unwrap_or(usize::MAX);
    let stream_bytes = maximum_stream
        .min(artifact_bytes)
        .max(FORWARD_RECORD_WIDTH_USIZE);
    let mut buffer = vec![0_u8; stream_bytes];
    let mut whole = crate::corruption_checksum::Checksum::new();
    let mut remaining = artifact.descriptor.bytes;
    let mut prior_uuid = None;
    while remaining != 0 {
        let read = usize::try_from(remaining.min(stream_bytes as u64))
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
        artifact
            .file
            .read_exact(&mut buffer[..read])
            .map_err(io_error)?;
        record_admission_read(metrics, read, buffer.len());
        whole.update(&buffer[..read]);
        for record in buffer[..read].chunks_exact(FORWARD_RECORD_WIDTH_USIZE) {
            let uuid: [u8; UUID_WIDTH_USIZE] =
                record[..UUID_WIDTH_USIZE].try_into().expect("fixed UUID");
            let node_id = u64::from_be_bytes(
                record[UUID_WIDTH_USIZE..]
                    .try_into()
                    .expect("fixed surrogate"),
            );
            if uuid == [0; UUID_WIDTH_USIZE]
                || prior_uuid.is_some_and(|prior| prior >= uuid)
                || !range_contains(ranges, node_id)
            {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "forward identity records are noncanonical",
                ));
            }
            prior_uuid = Some(uuid);
            commitment.add(&uuid, node_id);
        }
        remaining -= read as u64;
    }
    finish_admission(artifact, &whole, metrics)
}

/// Whether consecutive packed UUIDs strictly ascend, including across the
/// block boundaries inside one contiguous read.
fn uuids_strictly_ascend(packed: &[u8]) -> bool {
    packed
        .chunks_exact(UUID_WIDTH_USIZE)
        .zip(packed.chunks_exact(UUID_WIDTH_USIZE).skip(1))
        .all(|(prior, next)| prior < next)
}

/// Prove that UUIDs increase strictly across every ordinal, reading each block
/// once and authenticating it against its required checksum. A block that fails
/// is refused, never counted as an inversion. Stops at the first inversion:
/// the answer cannot change, and no value from later blocks is used.
fn scan_uuid_order(
    ranges: &mut [OpenRange],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<bool, V4OrdinalIdentityError> {
    let mut prior: Option<[u8; UUID_WIDTH_USIZE]> = None;
    let mut buffer = vec![0_u8; ORDINAL_BLOCK_BYTES_USIZE];
    for range in ranges {
        range
            .artifact
            .file
            .seek(SeekFrom::Start(0))
            .map_err(io_error)?;
        for block in &range.descriptor.blocks {
            let length = usize::try_from(block.count * UUID_WIDTH)
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            let capacity = buffer.len();
            let bytes = buffer
                .get_mut(..length)
                .ok_or(V4OrdinalIdentityError::Authentication)?;
            range.artifact.file.read_exact(bytes).map_err(io_error)?;
            record_admission_read(metrics, length, capacity);
            if crate::corruption_checksum::checksum(bytes) != block.xxh64 {
                return Err(V4OrdinalIdentityError::Authentication);
            }
            for record in bytes.chunks_exact(UUID_WIDTH_USIZE) {
                let uuid: [u8; UUID_WIDTH_USIZE] = record.try_into().expect("fixed UUID");
                if uuid == [0; UUID_WIDTH_USIZE] {
                    return Err(V4OrdinalIdentityError::InvalidDescriptor(
                        "ordinal UUID is zero",
                    ));
                }
                if prior.is_some_and(|prior| prior >= uuid) {
                    return Ok(false);
                }
                prior = Some(uuid);
            }
        }
    }
    Ok(true)
}

/// Prove UUID uniqueness across independently sorted forward generations with
/// one cursor per retained run. This is bounded by descriptor/run count rather
/// than graph cardinality and performs only sequential reads.
fn validate_unique_forward_runs(
    runs: &mut [OpenArtifact],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    let per_run_buffer = (STREAM_BYTES / runs.len().max(1) / FORWARD_RECORD_WIDTH_USIZE).max(1)
        * FORWARD_RECORD_WIDTH_USIZE;
    let buffer_lengths = runs
        .iter()
        .map(|run| {
            usize::try_from(run.descriptor.bytes)
                .unwrap_or(usize::MAX)
                .min(per_run_buffer)
                .max(FORWARD_RECORD_WIDTH_USIZE)
        })
        .collect::<Vec<_>>();
    let cursor_bytes =
        buffer_lengths
            .iter()
            .sum::<usize>()
            .saturating_add(runs.len().saturating_mul(
                std::mem::size_of::<ForwardRunCursor>()
                    + std::mem::size_of::<Reverse<([u8; UUID_WIDTH_USIZE], usize)>>(),
            ));
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
        metrics
            .retained_descriptor_bytes
            .saturating_add(metrics.manifest_bytes)
            .saturating_add(cursor_bytes as u64),
    );
    let mut cursors = runs
        .iter()
        .zip(buffer_lengths)
        .map(|(run, buffer_len)| ForwardRunCursor {
            buffer: vec![0; buffer_len],
            cursor: 0,
            valid: 0,
            remaining: run.descriptor.bytes,
        })
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(runs.len());
    for (index, run) in runs.iter_mut().enumerate() {
        run.file.rewind().map_err(io_error)?;
        if let Some(uuid) = cursors[index].next_uuid(run, metrics)? {
            heap.push(Reverse((uuid, index)));
        }
    }
    let mut prior = None;
    while let Some(Reverse((uuid, index))) = heap.pop() {
        if prior == Some(uuid) {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward identity UUID is repeated across generations",
            ));
        }
        prior = Some(uuid);
        if let Some(next) = cursors[index].next_uuid(&mut runs[index], metrics)? {
            heap.push(Reverse((next, index)));
        }
    }
    Ok(())
}

struct ForwardRunCursor {
    buffer: Vec<u8>,
    cursor: usize,
    valid: usize,
    remaining: u64,
}

impl ForwardRunCursor {
    fn next_uuid(
        &mut self,
        run: &mut OpenArtifact,
        metrics: &mut V4OrdinalAdmissionMetrics,
    ) -> Result<Option<[u8; UUID_WIDTH_USIZE]>, V4OrdinalIdentityError> {
        if self.cursor == self.valid {
            if self.remaining == 0 {
                return Ok(None);
            }
            self.valid = usize::try_from(self.remaining.min(self.buffer.len() as u64))
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            run.file
                .read_exact(&mut self.buffer[..self.valid])
                .map_err(io_error)?;
            record_admission_read(metrics, self.valid, self.buffer.len());
            self.remaining -= self.valid as u64;
            self.cursor = 0;
        }
        let record = &self.buffer[self.cursor..self.cursor + FORWARD_RECORD_WIDTH_USIZE];
        self.cursor += FORWARD_RECORD_WIDTH_USIZE;
        Ok(Some(
            record[..UUID_WIDTH_USIZE].try_into().expect("fixed UUID"),
        ))
    }
}
fn admit_tombstone_artifact(
    artifact: &mut OpenArtifact,
    blocks: &[V4OrdinalTombstoneBlock],
    ranges: &[OpenRange],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    artifact.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let block_bytes = TOMBSTONE_BLOCK_BYTES.min(artifact.descriptor.bytes.max(TOMBSTONE_WIDTH));
    let block_bytes =
        usize::try_from(block_bytes).map_err(|_| V4OrdinalIdentityError::Authentication)?;
    let mut buffer = vec![0_u8; block_bytes];
    let mut whole = crate::corruption_checksum::Checksum::new();
    let mut prior = None;
    let mut offset = 0_u64;
    let mut declared = blocks.iter();
    loop {
        let read = read_fill_or_eof(&mut artifact.file, &mut buffer, metrics)?;
        if read == 0 {
            break;
        }
        whole.update(&buffer[..read]);
        if !read.is_multiple_of(TOMBSTONE_WIDTH_USIZE) {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let mut first = None;
        let mut last = 0;
        for record in buffer[..read].chunks_exact(TOMBSTONE_WIDTH_USIZE) {
            let id = u64::from_be_bytes(record.try_into().expect("fixed tombstone"));
            if id == 0 || prior.is_some_and(|value| value >= id) || !range_contains(ranges, id) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "tombstone IDs are noncanonical",
                ));
            }
            first.get_or_insert(id);
            last = id;
            prior = Some(id);
        }
        if let Some(first) = first
            && !declared.next().is_some_and(|block| {
                block.offset == offset
                    && block.count == read as u64 / TOMBSTONE_WIDTH
                    && block.first == first
                    && block.last == last
                    && block.xxh64 == crate::corruption_checksum::checksum(&buffer[..read])
            })
        {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "tombstone block fences are noncanonical",
            ));
        }
        offset = offset.saturating_add(read as u64);
    }
    if declared.next().is_some() {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "tombstone block fences are noncanonical",
        ));
    }
    finish_admission(artifact, &whole, metrics)
}
fn read_tombstone_block(
    run: &mut OpenTombstones,
    run_index: usize,
    block_index: usize,
    maximum_cache_bytes: usize,
    request_buffer_bytes: u64,
    cache: &mut TombstoneBlockCache,
    metrics: &mut V4OrdinalLookupMetrics,
) -> Result<Vec<u64>, V4OrdinalIdentityError> {
    let block = run.blocks[block_index].clone();
    let cache_key = (run_index, block.offset);
    if let Some(ids) = cache.entries.get(&cache_key) {
        let clone_charge = ids.capacity().saturating_mul(std::mem::size_of::<u64>());
        metrics.retained_cache_bytes = metrics.retained_cache_bytes.max(cache.charged_bytes as u64);
        metrics.transient_buffer_bytes = metrics.transient_buffer_bytes.max(clone_charge as u64);
        metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
            request_buffer_bytes
                .saturating_add(cache.charged_bytes as u64)
                .saturating_add(clone_charge as u64),
        );
        return Ok(ids.clone());
    }
    let bytes_len = block
        .count
        .checked_mul(TOMBSTONE_WIDTH)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or(V4OrdinalIdentityError::Authentication)?;
    let mut bytes = vec![0_u8; bytes_len];
    run.artifact
        .file
        .seek(SeekFrom::Start(block.offset))
        .map_err(io_error)?;
    run.artifact.file.read_exact(&mut bytes).map_err(io_error)?;
    if crate::corruption_checksum::checksum(&bytes) != block.xxh64 {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, bytes_len as u64, 1);
    metrics.sequential_read_calls = metrics.sequential_read_calls.saturating_add(1);
    metrics.bytes_read = metrics.bytes_read.saturating_add(bytes_len as u64);
    let ids = bytes
        .chunks_exact(TOMBSTONE_WIDTH_USIZE)
        .map(|record| u64::from_be_bytes(record.try_into().expect("fixed tombstone")))
        .collect::<Vec<_>>();
    // The fence is descriptor authority and the checksum proved the bytes are
    // the writer's; this proves the decoded block still agrees with its fence
    // before any ID from it is used. (That every ID names a known ordinal is
    // a cross-artifact property, proven by `admit_complete`.)
    if ids.first() != Some(&block.first)
        || ids.last() != Some(&block.last)
        || ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "tombstone block fences are noncanonical",
        ));
    }
    let decoded_charge = ids.capacity().saturating_mul(std::mem::size_of::<u64>());
    let retained_entry_charge = TombstoneBlockCache::entry_charge(&ids);
    let transient_charge = bytes
        .capacity()
        .saturating_add(decoded_charge.saturating_mul(2))
        .saturating_add(TOMBSTONE_CACHE_ENTRY_CHARGE);
    metrics.transient_buffer_bytes = metrics.transient_buffer_bytes.max(transient_charge as u64);
    // The input byte buffer, decoded result, insertion clone, retained cache,
    // and prospective map/queue allocator metadata can coexist.
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
        request_buffer_bytes
            .saturating_add(cache.charged_bytes as u64)
            .saturating_add(bytes.capacity() as u64)
            .saturating_add((decoded_charge as u64).saturating_mul(2))
            .saturating_add(TOMBSTONE_CACHE_ENTRY_CHARGE as u64),
    );
    if TOMBSTONE_CACHE_FIXED_CHARGE.saturating_add(retained_entry_charge) <= maximum_cache_bytes {
        while cache.charged_bytes.saturating_add(retained_entry_charge) > maximum_cache_bytes {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            if let Some(evicted) = cache.entries.remove(&oldest) {
                cache.charged_bytes = cache
                    .charged_bytes
                    .saturating_sub(TombstoneBlockCache::entry_charge(&evicted));
            }
        }
        cache.entries.insert(cache_key, ids.clone());
        cache.order.push_back(cache_key);
        cache.charged_bytes = cache.charged_bytes.saturating_add(retained_entry_charge);
        metrics.retained_cache_bytes = metrics.retained_cache_bytes.max(cache.charged_bytes as u64);
    }
    Ok(ids)
}

fn range_contains(ranges: &[OpenRange], id: u64) -> bool {
    let index = ranges.partition_point(|range| range.descriptor.first_node_id <= id);
    index > 0
        && ranges[index - 1]
            .descriptor
            .first_node_id
            .checked_add(ranges[index - 1].descriptor.count)
            .is_some_and(|end| id < end)
}

fn plain_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

/// An immutable artifact is private (one link) or, when hydration hard-linked
/// it from the content-addressed store, a read-only shared inode. A writable
/// shared inode is refused: another name could rewrite it under this handle.
/// Neither case is the integrity boundary; every block read is checked against
/// its manifest digest, and the retained stamp detects cooperative change.
fn has_admissible_links(file: &File) -> Result<bool, V4OrdinalIdentityError> {
    let links = file_link_count(file).map_err(io_error)?;
    Ok(links == 1 || links > 1 && file.metadata().map_err(io_error)?.permissions().readonly())
}

fn open_admission_file(
    root: &StableDirectory,
    descriptor: &V4OrdinalArtifact,
) -> Result<OpenArtifact, V4OrdinalIdentityError> {
    let file = root
        .open_child_file(descriptor.name.as_ref())
        .map_err(io_error)?;
    let stamp = artifact_stamp(&file)?;
    if !has_admissible_links(&file)? || stamp.length != descriptor.bytes {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    Ok(OpenArtifact {
        file,
        stamp,
        descriptor: descriptor.clone(),
    })
}

fn record_admission_read(metrics: &mut V4OrdinalAdmissionMetrics, read: usize, capacity: usize) {
    crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, read as u64, 1);
    metrics.sequential_read_calls = metrics.sequential_read_calls.saturating_add(1);
    metrics.authenticated_bytes = metrics.authenticated_bytes.saturating_add(read as u64);
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
        metrics
            .retained_descriptor_bytes
            .saturating_add(metrics.manifest_bytes)
            .saturating_add(capacity as u64)
            .saturating_add(ADMISSION_TRANSIENT_FIXED_CHARGE),
    );
}

fn read_fill_or_eof<R: Read>(
    reader: &mut R,
    buffer: &mut [u8],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<usize, V4OrdinalIdentityError> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..]).map_err(io_error)?;
        if read == 0 {
            break;
        }
        filled += read;
        record_admission_read(metrics, read, buffer.len());
    }
    Ok(filled)
}

fn finish_admission(
    artifact: &mut OpenArtifact,
    digest: &crate::corruption_checksum::Checksum,
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    if digest.finish() != artifact.descriptor.xxh64
        || artifact.file.metadata().map_err(io_error)?.len() != artifact.descriptor.bytes
    {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    artifact.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    metrics.artifacts = metrics.artifacts.saturating_add(1);
    Ok(())
}
fn artifact_stamp(file: &File) -> Result<ArtifactStamp, V4OrdinalIdentityError> {
    let metadata = file.metadata().map_err(io_error)?;
    Ok(ArtifactStamp {
        identity: file_identity(file).map_err(io_error)?,
        length: metadata.len(),
        modified: metadata.modified().map_err(io_error)?,
    })
}

/// Check every block of one contiguous read against its required checksum, and
/// against the recorded order when a caller relies on it, before any value from
/// the read is used.
fn authenticate_run(
    range: &OpenRange,
    blocks: std::ops::RangeInclusive<usize>,
    buffer: &[u8],
) -> Result<(), V4OrdinalIdentityError> {
    let run_start = range.descriptor.blocks[*blocks.start()].offset;
    for block in &range.descriptor.blocks[blocks] {
        let slice_start = usize::try_from(block.offset - run_start)
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
        let slice_len = usize::try_from(block.count * UUID_WIDTH)
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
        if crate::corruption_checksum::checksum(&buffer[slice_start..slice_start + slice_len])
            != block.xxh64
        {
            return Err(V4OrdinalIdentityError::Authentication);
        }
    }
    if range.verify_order && !uuids_strictly_ascend(buffer) {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal UUIDs contradict the recorded UUID order",
        ));
    }
    Ok(())
}

/// With a recorded order relied on, adjacent blocks this handle holds must
/// meet in ascending order: a block is only checked internally when read, so
/// the seam between two held blocks is checked here, from memory.
fn check_cached_seams(
    range: &OpenRange,
    range_index: usize,
    cache: &OrdinalBlockCache,
    touched: &[usize],
) -> Result<(), V4OrdinalIdentityError> {
    if !range.verify_order {
        return Ok(());
    }
    let seam_inverted = |lower: usize, upper: usize| match (
        cache.entries.get(&(range_index, lower)),
        cache.entries.get(&(range_index, upper)),
    ) {
        (Some(lower), Some(upper)) => {
            lower.bytes[lower.bytes.len() - UUID_WIDTH_USIZE..] >= upper.bytes[..UUID_WIDTH_USIZE]
        }
        _ => false,
    };
    for &index in touched {
        if (index > 0 && seam_inverted(index - 1, index)) || seam_inverted(index, index + 1) {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal UUIDs contradict the recorded UUID order",
            ));
        }
    }
    Ok(())
}

/// Answer from memory every selected block this handle already authenticated,
/// and return the ones that still need reading. A held block that contradicts
/// a relied-on recorded order is refused exactly as a fresh read would be.
fn serve_cached_blocks(
    range: &OpenRange,
    selected: Vec<usize>,
    ids: &[u64],
    (range_index, cache): (usize, &OrdinalBlockCache),
    resolved: &mut BTreeMap<u64, Uuid>,
) -> Result<Vec<usize>, V4OrdinalIdentityError> {
    let first = range.descriptor.first_node_id;
    let mut uncached = Vec::with_capacity(selected.len());
    for index in selected {
        let block = &range.descriptor.blocks[index];
        let Some(cached) = cache.entries.get(&(range_index, index)) else {
            uncached.push(index);
            continue;
        };
        if range.verify_order && !cached.ascends {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal UUIDs contradict the recorded UUID order",
            ));
        }
        resolve_block_ids(first, block, block.offset, &cached.bytes, ids, resolved)?;
    }
    Ok(uncached)
}

fn read_range_coalesced(
    range: &mut OpenRange,
    ids: &[u64],
    read: RangeRead<'_>,
    resolved: &mut BTreeMap<u64, Uuid>,
    metrics: &mut V4OrdinalLookupMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    let RangeRead {
        range_index,
        cache,
        max_cache_bytes,
        gap_bytes,
        maximum_read_bytes,
        retained_buffer_bytes,
    } = read;
    let read_cache = (range_index, &*cache);
    let first = range.descriptor.first_node_id;
    let selected = range
        .descriptor
        .blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            let block_first = first + block.offset / UUID_WIDTH;
            let block_last = block_first + block.count - 1;
            (ids.partition_point(|id| *id < block_first)
                != ids.partition_point(|id| *id <= block_last))
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let touched = selected.clone();
    let selected = serve_cached_blocks(range, selected, ids, read_cache, resolved)?;
    let mut selected_at = 0;
    while selected_at < selected.len() {
        let first_index = selected[selected_at];
        let mut last_index = first_index;
        let mut next = selected_at + 1;
        while next < selected.len() {
            let candidate = selected[next];
            let prior = &range.descriptor.blocks[last_index];
            let block = &range.descriptor.blocks[candidate];
            let prior_end = prior.offset + prior.count * UUID_WIDTH;
            let combined = block.offset + block.count * UUID_WIDTH
                - range.descriptor.blocks[first_index].offset;
            if block.offset.saturating_sub(prior_end) > gap_bytes
                || combined > maximum_read_bytes as u64
            {
                break;
            }
            last_index = candidate;
            next += 1;
        }
        let first_block = &range.descriptor.blocks[first_index];
        let last_block = &range.descriptor.blocks[last_index];
        let bytes = last_block.offset + last_block.count * UUID_WIDTH - first_block.offset;
        let length = usize::try_from(bytes).map_err(|_| V4OrdinalIdentityError::Authentication)?;
        if length > maximum_read_bytes {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal block exceeds read bound",
            ));
        }
        let mut buffer = vec![0_u8; length];
        range
            .artifact
            .file
            .seek(SeekFrom::Start(first_block.offset))
            .map_err(io_error)?;
        range
            .artifact
            .file
            .read_exact(&mut buffer)
            .map_err(io_error)?;
        crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, bytes, 1);
        metrics.sequential_read_calls = metrics.sequential_read_calls.saturating_add(1);
        metrics.bytes_read = metrics.bytes_read.saturating_add(bytes);
        // The read buffer and, while it is split into held blocks, their copies.
        let copies = if max_cache_bytes == 0 { 0 } else { bytes };
        metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
            retained_buffer_bytes
                .saturating_add(bytes)
                .saturating_add(copies),
        );
        authenticate_run(range, first_index..=last_index, &buffer)?;
        for (offset, block) in range.descriptor.blocks[first_index..=last_index]
            .iter()
            .enumerate()
        {
            resolve_block_ids(first, block, first_block.offset, &buffer, ids, resolved)?;
            let slice_start = usize::try_from(block.offset - first_block.offset)
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            let slice_len = usize::try_from(block.count * UUID_WIDTH)
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            let bytes = buffer[slice_start..slice_start + slice_len].to_vec();
            let ascends = uuids_strictly_ascend(&bytes);
            cache.insert(
                (range_index, first_index + offset),
                CachedOrdinalBlock { bytes, ascends },
                max_cache_bytes,
            );
        }
        selected_at = next;
    }
    check_cached_seams(range, range_index, cache, &touched)
}

/// Resolve the requested IDs that fall in `block` from `buffer`, which holds
/// the block's bytes starting `buffer_start` bytes into the artifact.
fn resolve_block_ids(
    first_node_id: u64,
    block: &V4OrdinalBlock,
    buffer_start: u64,
    buffer: &[u8],
    ids: &[u64],
    resolved: &mut BTreeMap<u64, Uuid>,
) -> Result<(), V4OrdinalIdentityError> {
    let block_first = first_node_id + block.offset / UUID_WIDTH;
    let block_last = block_first + block.count - 1;
    let start = ids.partition_point(|id| *id < block_first);
    let end = ids.partition_point(|id| *id <= block_last);
    for id in &ids[start..end] {
        let at = usize::try_from(block.offset - buffer_start + (id - block_first) * UUID_WIDTH)
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
        let uuid = Uuid::from_bytes(
            buffer[at..at + UUID_WIDTH_USIZE]
                .try_into()
                .expect("fixed UUID"),
        );
        resolved.insert(*id, uuid);
    }
    Ok(())
}

fn read_bounded(file: &mut File, maximum: u64) -> Result<Vec<u8>, V4OrdinalIdentityError> {
    let length = file.seek(SeekFrom::End(0)).map_err(io_error)?;
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    if length > maximum {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "manifest exceeds size bound",
        ));
    }
    let capacity = usize::try_from(length).map_err(|_| V4OrdinalIdentityError::Authentication)?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("writing to a string cannot fail");
            encoded
        },
    )
}

fn io_error(error: impl std::fmt::Display) -> V4OrdinalIdentityError {
    let _ = error;
    V4OrdinalIdentityError::Io
}

#[cfg(test)]
mod tests;
